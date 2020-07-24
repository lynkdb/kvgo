// Copyright 2015 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvgo

import (
	"context"
	"strconv"
	"time"

	"github.com/hooto/hlog4g/hlog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	kv2 "github.com/lynkdb/kvspec/v2"
)

func (cn *Conn) workerLocal() {

	for !cn.close {

		if err := cn.workerLocalExpiredRefresh(); err != nil {
			hlog.Printf("warn", "local ttl clean err %s", err.Error())
		}

		if err := cn.workerLocalTableRefresh(); err != nil {
			hlog.Printf("warn", "local table refresh err %s", err.Error())
		}

		time.Sleep(workerLocalExpireSleep)
	}
}

func (cn *Conn) workerClusterReplica() {

	for !cn.close {
		time.Sleep(workerClusterSleep)

		if err := cn.workerClusterReplicaLogAsync(); err != nil {
			hlog.Printf("warn", "cluster log async err %s", err.Error())
		}
	}
}

func (cn *Conn) workerLocalExpiredRefresh() error {

	for _, t := range cn.tables {
		if err := cn.workerLocalExpiredRefreshTable(t); err != nil {
			hlog.Printf("warn", "cluster ttl refresh error %s", err.Error())
		}
	}

	return nil
}

func (cn *Conn) workerLocalExpiredRefreshTable(dt *dbTable) error {

	iter := dt.db.NewIterator(&util.Range{
		Start: keyEncode(nsKeyTtl, uint64ToBytes(0)),
		Limit: keyEncode(nsKeyTtl, uint64ToBytes(uint64(time.Now().UnixNano()/1e6))),
	}, nil)
	defer iter.Release()

	for !cn.close {

		var (
			num   = 0
			batch = new(leveldb.Batch)
		)

		for iter.Next() {

			meta, err := kv2.ObjectMetaDecode(bytesClone(iter.Value()))
			if err != nil {
				return err
			}

			data, err := dt.db.Get(keyEncode(nsKeyMeta, meta.Key), nil)
			if err == nil {

				cmeta, err := kv2.ObjectMetaDecode(data)
				if err == nil && cmeta.Version == meta.Version {
					batch.Delete(keyEncode(nsKeyMeta, meta.Key))
					batch.Delete(keyEncode(nsKeyData, meta.Key))
					batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
				}

			} else if err.Error() != ldbNotFound {
				return err
			}

			batch.Delete(keyExpireEncode(nsKeyTtl, meta.Expired, meta.Key))
			num += 1

			if num >= workerLocalExpireLimit {
				break
			}

			if cn.close {
				break
			}
		}

		if num > 0 {
			dt.db.Write(batch, nil)
		}

		if num < workerLocalExpireLimit {
			break
		}
	}

	return nil
}

func (cn *Conn) workerClusterReplicaLogAsync() error {

	if len(cn.opts.Cluster.Masters) < 1 {
		return nil
	}

	for _, hp := range cn.opts.Cluster.Masters {

		if cn.close {
			break
		}

		if hp.Addr == cn.opts.Server.Bind {
			continue
		}

		for _, dt := range cn.tables {

			if err := cn.workerClusterReplicaLogAsyncTable(hp, dt); err != nil {
				//
			}

			if cn.close {
				break
			}
		}

	}

	return nil
}

func (cn *Conn) workerClusterReplicaLogAsyncTable(hp *ConfigClusterMaster, dt *dbTable) error {

	var (
		offset = uint64(0)
		num    = int64(0)
	)

	if bs, err := dt.db.Get(keySysLogAsync(hp.Addr), nil); err != nil {
		if err.Error() != ldbNotFound {
			return err
		}
	} else {
		if offset, err = strconv.ParseUint(string(bs), 10, 64); err != nil {
			return err
		}
	}

	conn, err := clientConn(hp.Addr, cn.authKey(hp.Addr), hp.AuthTLSCert)
	if err != nil {
		return err
	}

	ctx, fc := context.WithTimeout(context.Background(), time.Second*10)
	defer fc()

	rr := kv2.NewObjectReader(nil).LogOffsetSet(offset).LimitNumSet(100)

	for !cn.close {

		rs, err := kv2.NewPublicClient(conn).Query(ctx, rr)
		if err != nil || !rs.OK() || len(rs.Items) < 1 {
			break
		}

		for _, item := range rs.Items {

			ow := &kv2.ObjectWriter{
				Meta: item.Meta,
				Data: item.Data,
			}

			if kv2.AttrAllow(item.Meta.Attrs, kv2.ObjectMetaAttrDelete) {
				ow.ModeDeleteSet(true)
			}

			if rs2 := cn.objectCommitLocal(ow, item.Meta.Version); rs2.OK() {
				rr.LogOffset = item.Meta.Version
				num += 1
			}
		}

		if !rs.Next {
			break
		}
	}

	if rr.LogOffset > offset {
		dt.db.Put(keySysLogAsync(hp.Addr),
			[]byte(strconv.FormatUint(rr.LogOffset, 10)), nil)
	}

	if num > 0 {
		hlog.Printf("debug", "kvgo log async num %d, ver %d", num, rr.LogOffset)
	}

	return nil
}

var (
	workerLocalTableRefreshLastTime int64 = 0
)

func (cn *Conn) workerLocalTableRefresh() error {

	tn := time.Now().Unix()
	if workerLocalTableRefreshLastTime+workerTableRefreshTime > tn {
		return nil
	}

	rgS := []util.Range{
		{
			Start: []byte{},
			Limit: []byte{0xff},
		},
	}
	rgK := &util.Range{
		Start: keyEncode(nsKeyMeta, []byte{}),
		Limit: keyEncode(nsKeyMeta, []byte{0xff}),
	}

	if cn.opts.Feature.WriteMetaDisable {
		rgK.Start = keyEncode(nsKeyData, []byte{})
		rgK.Limit = keyEncode(nsKeyData, []byte{0xff})
	}

	for _, t := range cn.tables {

		s, err := t.db.SizeOf(rgS)
		if err != nil {
			hlog.Printf("warn", "get db size error %s", err.Error())
			continue
		}
		if len(s) < 1 {
			continue
		}

		kn := uint64(0)
		iter := t.db.NewIterator(rgK, nil)
		for ; iter.Next(); kn++ {
		}
		iter.Release()

		rr := kv2.NewObjectWriter(nsSysTableStatus(t.tableName), &kv2.TableStatus{
			Name:   t.tableName,
			KeyNum: kn,
			DbSize: uint64(s[0]),
		}).TableNameSet(sysTableName)

		rs := cn.objectCommitLocal(rr, 0)
		if !rs.OK() {
			hlog.Printf("warn", "refresh table (%s) status error %s", t.tableName, err.Error())
		}

		if cn.close {
			break
		}
	}

	workerLocalTableRefreshLastTime = tn

	return nil
}
