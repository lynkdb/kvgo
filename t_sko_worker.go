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

	"github.com/lynkdb/iomix/sko"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (cn *SkoConn) skoClusterWorker() {

	for {
		time.Sleep(1e9)

		if err := cn.skoClusterLogAsync(); err != nil {
			//
		}
	}
}

func (cn *SkoConn) skoClusterLogAsync() error {

	if cn.skoCluster == nil {
		return nil
	}

	ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
	defer fc()

	for _, hp := range cn.opts.ClusterNodes {

		if hp == cn.opts.ClusterBind {
			continue
		}

		var (
			offset = uint64(0)
		)

		if bs, err := cn.db.Get(skoKeySysLogAsync(hp), nil); err != nil {
			if err.Error() != ldbNotFound {
				return err
			}
		} else {
			if offset, err = strconv.ParseUint(string(bs), 10, 64); err != nil {
				return nil
			}
		}

		conn, err := ClientConn(hp)
		if err != nil {
			continue
		}

		rr := sko.NewObjectReader(nil).LogOffsetSet(offset).LimitNumSet(100)

		for {

			rs, err := sko.NewObjectClient(conn).Query(ctx, rr)
			if err != nil {
				break
			}

			if !rs.OK() || len(rs.Items) < 1 {
				break
			}

			for _, item := range rs.Items {

				ow := &sko.ObjectWriter{
					Meta: item.Meta,
					Data: item.Data,
				}

				if sko.AttrAllow(item.Meta.Attrs, sko.ObjectMetaAttrDelete) {
					ow.ModeDeleteSet(true)
				}

				if rs2 := cn.objectCommitLocal(ow, item.Meta.Version); rs2.OK() {
					rr.LogOffset = item.Meta.Version
				}
			}

			if !rs.Next {
				break
			}
		}

		if rr.LogOffset > offset {
			cn.db.Put(skoKeySysLogAsync(hp),
				[]byte(strconv.FormatUint(rr.LogOffset, 10)), nil)
		}
	}

	return nil
}

func (cn *SkoConn) skoWorker() {
	for {
		time.Sleep(200e6)
		cn.skoWorkerExpiredRefresh()
	}
}

func (cn *SkoConn) skoWorkerExpiredRefresh() error {

	for {

		var (
			batch = new(leveldb.Batch)
			iter  = cn.db.NewIterator(&util.Range{
				Start: t_ns_cat(ns_sko_ttl, uint64_to_bytes(0)),
				Limit: t_ns_cat(ns_sko_ttl, uint64_to_bytes(uint64(time.Now().UnixNano()/1e6))),
			}, nil)
			num = 0
		)

		for iter.Next() {

			meta, err := sko.ObjectMetaDecode(bytes_clone(iter.Value()))
			if err != nil {
				break
			}

			data, err := cn.db.Get(t_ns_cat(ns_sko_meta, meta.Key), nil)
			if err == nil {

				cmeta, err := sko.ObjectMetaDecode(data)
				if err == nil && cmeta.Version == meta.Version {
					batch.Delete(t_ns_cat(ns_sko_meta, meta.Key))
					batch.Delete(t_ns_cat(ns_sko_data, meta.Key))
					batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
				}

			} else if err.Error() != ldbNotFound {
				break
			}

			batch.Delete(keyExpireEncode(ns_sko_ttl, meta.Expired, meta.Key))
			num += 1

			if num >= ttl_worker_limit {
				break
			}
		}

		iter.Release()

		if num > 0 {
			cn.db.Write(batch, nil)
		}

		if num < ttl_worker_limit {
			break
		}
	}

	return nil
}
