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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hooto/hlog4g/hlog"
	ps_disk "github.com/shirou/gopsutil/disk"
	ps_mem "github.com/shirou/gopsutil/mem"
	"github.com/valuedig/apis/go/tsd/v1"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

func (cn *Conn) workerLocal() {
	cn.workmu.Lock()
	if cn.workerLocalRunning {
		cn.workmu.Unlock()
		return
	}
	cn.workerLocalRunning = true
	cn.workmu.Unlock()

	go cn.workerLocalReplicaOfRefresh()

	for !cn.close {

		if err := cn.workerLocalExpiredRefresh(); err != nil {
			hlog.Printf("warn", "local ttl clean err %s", err.Error())
		}

		if err := cn.workerLocalTableRefresh(); err != nil {
			hlog.Printf("warn", "local table refresh err %s", err.Error())
		}

		if err := cn.workerLocalSysStatusRefresh(); err != nil {
			hlog.Printf("warn", "local sys-status refresh err %s", err.Error())
		}

		time.Sleep(workerLocalExpireSleep)
	}
}

func (cn *Conn) workerLocalReplicaOfRefresh() {

	hlog.Printf("info", "replica-of servers %d", len(cn.opts.Cluster.ReplicaOfNodes))

	for !cn.close {
		time.Sleep(workerReplicaLogPullSleep)

		if err := cn.workerLocalReplicaOfLogPull(); err != nil {
			hlog.Printf("warn", "replica-of log async err %s", err.Error())
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

	tn := timems()

	if tn < dt.expiredSync(0) {
		return nil
	}

	var (
		offset = keyEncode(nsKeyTtl, uint64ToBytes(0))
		cutset = keyEncode(nsKeyTtl, uint64ToBytes(uint64(tn)))
		iter   = dt.db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})
		ok bool
	)
	defer iter.Release()

	for !cn.close {

		batch := dt.db.NewBatch()

		for ok = iter.First(); ok && !cn.close; ok = iter.Next() {

			if bytes.Compare(iter.Key(), offset) <= 0 {
				hlog.Printf("info", "ttl skip %v", iter.Key())
				continue
			}

			if bytes.Compare(iter.Key(), cutset) > 0 {
				hlog.Printf("info", "ttl break %v", iter.Key())
				break
			}

			meta, err := kv2.ObjectMetaDecode(bytesClone(iter.Value()))
			if err != nil {
				hlog.Printf("warn", "db err %s", err.Error())
				break
			}

			for _, dbkey := range [][]byte{
				keyEncode(nsKeyMeta, meta.Key),
				keyEncode(nsKeyData, meta.Key),
			} {
				if ss := dt.db.Get(dbkey, nil); ss.OK() {
					cmeta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && cmeta.Version == meta.Version {
						batch.Delete(dbkey)
					}
				}
			}

			batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
			batch.Delete(keyExpireEncode(nsKeyTtl, meta.Expired, meta.Key))

			if batch.Len() >= workerLocalExpireLimit {
				break
			}
		}

		bn := batch.Len()

		if bn > 0 {
			err := batch.Commit()
			hlog.Printf("debug", "table %s, ttl clean %d, err %v",
				dt.tableName, bn, err)
		} else {
			dt.expiredSync(tn + 1000)
		}

		if bn < workerLocalExpireLimit {
			break
		}
	}

	return nil
}

func (cn *Conn) workerLocalTableRefresh() error {

	tn := time.Now().Unix()

	if (cn.workerTableRefreshed + workerTableRefreshTime) > tn {
		return nil
	}

	rgS := []*kv2.StorageIteratorRange{
		{
			Start: []byte{},
			Limit: []byte{0xff},
		},
	}
	rgK := &kv2.StorageIteratorRange{
		Start: keyEncode(nsKeyMeta, []byte{}),
		Limit: keyEncode(nsKeyMeta, []byte{0xff}),
	}
	rgIncr := &kv2.StorageIteratorRange{
		Start: keySysIncrCutset(""),
		Limit: append(keySysIncrCutset(""), []byte{0xff}...),
	}
	rgPull := &kv2.StorageIteratorRange{
		Start: keySysLogPull("", ""),
		Limit: append(keySysLogPull("", ""), []byte{0xff}...),
	}

	if cn.opts.Feature.WriteMetaDisable {
		rgK.Start = keyEncode(nsKeyData, []byte{})
		rgK.Limit = keyEncode(nsKeyData, []byte{0xff})
	}

	for _, t := range cn.tables {

		if err := cn.workerLocalLogCleanTable(t); err != nil {
			hlog.Printf("warn", "worker log clean table %s, err %s",
				t.tableName, err.Error())
		}

		// db size
		s, err := t.db.SizeOf(rgS)
		if err != nil {
			hlog.Printf("warn", "get db size error %s", err.Error())
			continue
		}
		if len(s) < 1 {
			continue
		}

		// db keys
		kn := uint64(0)
		iter := t.db.NewIterator(rgK)
		for ok := iter.First(); ok; ok = iter.Next() {
			kn++
		}
		iter.Release()

		tableStatus := kv2.TableStatus{
			Name:    t.tableName,
			KeyNum:  kn,
			DbSize:  uint64(s[0]),
			Options: map[string]int64{},
		}

		// log-id
		if ss := t.db.Get(keySysLogCutset, nil); ss.OK() {
			if logid, err := strconv.ParseInt(ss.String(), 10, 64); err == nil {
				tableStatus.Options["log_id"] = logid
			}
		}

		// incr
		iterIncr := t.db.NewIterator(rgIncr)
		for ok := iterIncr.First(); ok; ok = iterIncr.Next() {

			if bytes.Compare(iterIncr.Key(), rgIncr.Start) <= 0 {
				continue
			}

			if bytes.Compare(iterIncr.Key(), rgIncr.Limit) > 0 {
				break
			}

			incrid, err := strconv.ParseInt(string(iterIncr.Value()), 10, 64)
			if err != nil {
				continue
			}

			key := bytes.TrimPrefix(iterIncr.Key(), rgIncr.Start)
			if len(key) > 0 {
				tableStatus.Options[fmt.Sprintf("incr_id_%s", string(key))] = incrid
			}
		}
		iterIncr.Release()

		// async
		iterPull := t.db.NewIterator(rgPull)
		for ok := iterPull.First(); ok; ok = iterPull.Next() {

			if bytes.Compare(iterPull.Key(), rgPull.Start) <= 0 {
				continue
			}

			if bytes.Compare(iterPull.Key(), rgPull.Limit) > 0 {
				break
			}

			logid, err := strconv.ParseInt(string(iterPull.Value()), 10, 64)
			if err != nil {
				continue
			}

			key := bytes.TrimPrefix(iterPull.Key(), rgPull.Start)
			if len(key) > 0 {
				tableStatus.Options[fmt.Sprintf("async_%s", string(key))] = logid
			}
		}
		iterPull.Release()

		//
		rr := kv2.NewObjectWriter(nsSysTableStatus(t.tableName), tableStatus).
			TableNameSet(sysTableName)
		rs := cn.commitLocal(rr, 0)
		if !rs.OK() {
			hlog.Printf("warn", "refresh table (%s) status error %s", t.tableName, rs.Message)
		}

		if cn.close {
			break
		}
	}

	cn.workerTableRefreshed = tn

	return nil
}

func (cn *Conn) workerLocalReplicaOfLogPull() error {

	ups := map[string]bool{}

	for _, hp := range cn.opts.Cluster.MainNodes {

		if cn.close {
			break
		}

		if hp.Addr == cn.opts.Server.Bind {
			continue
		}

		if _, ok := ups[hp.Addr]; ok {
			continue
		}

		for _, dt := range cn.tables {

			go func(hp *ClientConfig, dt *dbTable) {
				if err := cn.workerLocalReplicaOfLogPullTable(hp, &ConfigReplicaTableMap{
					From: dt.tableName,
					To:   dt.tableName,
				}); err != nil {
					hlog.Printf("warn", "worker replica-of log-async table %s -> %s, err %s",
						dt.tableName, dt.tableName, err.Error())
				}
			}(hp, dt)

			if cn.close {
				break
			}
		}

		ups[hp.Addr] = true
	}

	for _, hp := range cn.opts.Cluster.ReplicaOfNodes {

		if cn.close {
			break
		}

		if hp.Addr == cn.opts.Server.Bind || len(hp.TableMaps) == 0 {
			continue
		}

		if _, ok := ups[hp.Addr]; ok {
			continue
		}

		for _, tm := range hp.TableMaps {

			go func(hp *ClientConfig, tm *ConfigReplicaTableMap) {
				if err := cn.workerLocalReplicaOfLogPullTable(hp, tm); err != nil {
					hlog.Printf("warn", "worker replica-of log-async table %s -> %s, err %s",
						tm.From, tm.To, err.Error())
				}
			}(hp.ClientConfig, tm)

			if cn.close {
				break
			}
		}

		ups[hp.Addr] = true
	}

	return nil
}

func (cn *Conn) workerLocalReplicaOfLogPullTable(hp *ClientConfig, tm *ConfigReplicaTableMap) error {

	if tm.From == "sys" || tm.To == "sys" {
		return nil
	}

	tdb := cn.tabledb(tm.To)
	if tdb == nil {
		return errors.New("no table found in local server")
	}

	lkey := hp.Addr + "/" + tm.From

	tdb.logPullMu.Lock()
	if _, ok := tdb.logPullPending[lkey]; ok {
		tdb.logPullMu.Unlock()
		return nil
	}
	tdb.logPullPending[lkey] = true
	tdb.logPullMu.Unlock()

	defer func() {
		tdb.logPullMu.Lock()
		delete(tdb.logPullPending, lkey)
		tdb.logPullMu.Unlock()
	}()

	var (
		offset = tdb.logPullOffsetFlush(hp.Addr, tm.From, nil, false)
		retry  = 0
		mttl   = uint64(3600) * 1000
	)

	if offset == nil {
		return errors.New("db error")
	}

	conn, err := clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, false)
	if err != nil {
		return err
	}

	if len(offset.DataKeyCutset) == 0 {
		offset.DataKeyCutset = bytes.Repeat([]byte{0xff}, 32)
	}

	if len(offset.MetaKeyCutset) == 0 {
		offset.MetaKeyCutset = bytes.Repeat([]byte{0xff}, 32)
	}

	if bytes.Compare(offset.DataKeyOffset, offset.DataKeyCutset) < 0 {

		var (
			statsScan = 0
			statsSync = 0
			req       = &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
				Attrs:     kv2.ObjectMetaAttrMetaOff,
			}
		)

		for !cn.close {

			req.KeyOffset = offset.DataKeyOffset

			ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
			rep, err := kv2.NewInternalClient(conn).LogSync(ctx, req)
			fc()

			if err != nil {
				retry++

				hlog.SlotPrint(600, "warn", "log sync pull from %s/%s, err %s, retry(%d) ...",
					hp.Addr, tdb.tableName, err.Error(), retry)

				time.Sleep(1e9)
				conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
				continue
			}
			retry = 0

			if cn.close {
				break
			}

			if offset.LogOffset == 0 {
				offset.LogOffset = rep.LogCutset
			}

			if len(rep.Logs) == 0 && offset.LogOffset > 0 {
				offset.DataKeyCutset = offset.DataKeyOffset
				break
			}

			statsScan += len(rep.Logs)

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			for _, v := range rep.Logs {

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && (meta.Version >= v.Version && (meta.Updated+mttl) >= v.Updated) {
						continue
					}
				}

				req2.Keys = append(req2.Keys, v)
			}

			for len(req2.Keys) > 0 {

				ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
				rep2, err := kv2.NewInternalClient(conn).LogSync(ctx, req2)
				fc()

				if err != nil {
					return err
				}

				for _, item := range rep2.Items {

					ow := &kv2.ObjectWriter{
						Meta: item.Meta,
						Data: item.Data,
					}

					if kv2.AttrAllow(item.Meta.Attrs, kv2.ObjectMetaAttrDelete) {
						ow.ModeDeleteSet(true)
					}

					ow.TableNameSet(tm.To)

					rs2 := cn.commitLocal(ow, item.Meta.Version)
					if !rs2.OK() {
						return rs2.Error()
					}
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys

				hlog.SlotPrint(600, "info", "log sync pull from %s/%s to local/%s, data keys sync/scan %d/%d, log offset %d, key offset %v",
					hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.DataKeyOffset))
			}

			if len(rep.Logs) > 0 {
				offset.DataKeyOffset = bytesClone(rep.Logs[len(rep.Logs)-1].Key)
			}

			tdb.logPullOffsetFlush(hp.Addr, tm.From, offset, true)
			hlog.SlotPrint(600, "info", "log sync pull from %s/%s to local/%s, data keys sync/scan %d/%d, log offset %d, key offset %v",
				hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.DataKeyOffset))
		}
	}

	if bytes.Compare(offset.MetaKeyOffset, offset.MetaKeyCutset) < 0 {

		var (
			statsScan = 0
			statsSync = 0
			req       = &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
				Attrs:     kv2.ObjectMetaAttrDataOff,
			}
		)

		for !cn.close {

			req.KeyOffset = offset.MetaKeyOffset

			ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
			rep, err := kv2.NewInternalClient(conn).LogSync(ctx, req)
			fc()

			if err != nil {
				retry++

				hlog.SlotPrint(600, "warn", "log sync pull from %s/%s, err %s, retry(%d) ...",
					hp.Addr, tdb.tableName, err.Error(), retry)

				time.Sleep(1e9)
				conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
				continue
			}
			retry = 0

			if cn.close {
				break
			}

			if offset.LogOffset == 0 {
				offset.LogOffset = rep.LogCutset
			}

			if len(rep.Logs) == 0 && offset.LogOffset > 0 {
				offset.MetaKeyCutset = offset.MetaKeyOffset
				break
			}

			statsScan += len(rep.Logs)

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			for _, v := range rep.Logs {

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && (meta.Version >= v.Version && (meta.Updated+mttl) >= v.Updated) {
						continue
					}
				}

				req2.Keys = append(req2.Keys, v)
			}

			for len(req2.Keys) > 0 {

				ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
				rep2, err := kv2.NewInternalClient(conn).LogSync(ctx, req2)
				fc()

				if err != nil {
					return err
				}

				for _, item := range rep2.Items {

					ow := &kv2.ObjectWriter{
						Meta: item.Meta,
						Data: item.Data,
					}

					if kv2.AttrAllow(item.Meta.Attrs, kv2.ObjectMetaAttrDelete) {
						ow.ModeDeleteSet(true)
					}

					ow.TableNameSet(tm.To)

					rs2 := cn.commitLocal(ow, item.Meta.Version)
					if !rs2.OK() {
						return rs2.Error()
					}
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys

				hlog.SlotPrint(600, "info", "log sync pull from %s/%s to local/%s, meta keys sync/scan %d/%d, log offset %d, key offset %v",
					hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.MetaKeyOffset))
			}

			if len(rep.Logs) > 0 {
				offset.MetaKeyOffset = bytesClone(rep.Logs[len(rep.Logs)-1].Key)
			}

			tdb.logPullOffsetFlush(hp.Addr, tm.From, offset, true)
			hlog.SlotPrint(600, "info", "log sync pull from %s/%s to local/%s, meta keys sync/scan %d/%d, log offset %d, key offset %v",
				hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.MetaKeyOffset))
		}
	}

	// hlog.Printf("info", "pull from %s/%s at %d", hp.Addr, tm.From, offset)

	var (
		statsScan = 0
		statsSync = 0
		req       = &kv2.LogSyncRequest{
			ServerId:  cn.opts.Server.ID,
			Addr:      cn.opts.Server.Bind,
			TableName: tm.From,
		}
	)

	for !cn.close {

		req.LogOffset = offset.LogOffset

		ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
		rep, err := kv2.NewInternalClient(conn).LogSync(ctx, req)
		fc()

		if err != nil {
			retry++

			hlog.SlotPrint(600, "warn", "log sync pull from %s/%s, err %s, retry(%d) ...",
				hp.Addr, tdb.tableName, err.Error(), retry)

			time.Sleep(1e9)
			conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
			continue
		}
		retry = 0

		if cn.close {
			break
		}

		if len(rep.Logs) > 0 {

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			for _, v := range rep.Logs {

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && (meta.Version >= v.Version && (meta.Updated+mttl) >= v.Updated) {
						continue
					}
				}

				req2.Keys = append(req2.Keys, v)
			}

			statsScan += len(rep.Logs)

			for len(req2.Keys) > 0 {

				ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
				rep2, err := kv2.NewInternalClient(conn).LogSync(ctx, req2)
				fc()

				if err != nil {
					return err
				}

				for _, item := range rep2.Items {

					ow := &kv2.ObjectWriter{
						Meta: item.Meta,
						Data: item.Data,
					}

					if kv2.AttrAllow(item.Meta.Attrs, kv2.ObjectMetaAttrDelete) {
						ow.ModeDeleteSet(true)
					}

					ow.TableNameSet(tm.To)

					rs2 := cn.commitLocal(ow, item.Meta.Version)
					if !rs2.OK() {
						return rs2.Error()
					}
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys
			}

			if len(rep.Logs) > 0 {
				offset.LogOffset = rep.Logs[len(rep.Logs)-1].Version
			}

			hlog.SlotPrint(600, "info", "log sync pull from %s/%s to local/%s, log keys sync/scan %d/%d, log offset %d",
				hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset)

		} else if rep.Action == 0 {
			time.Sleep(1e9)
		}

		if offset.LogOffset > req.LogOffset {
			req.LogOffset = offset.LogOffset
			tdb.logPullOffsetFlush(hp.Addr, tm.From, offset, true)
		}
	}

	return nil
}

func (cn *Conn) workerLocalLogCleanTable(tdb *dbTable) error {

	var (
		offset = keyEncode(nsKeyLog, uint64ToBytes(0))
		cutset = keyEncode(nsKeyLog, []byte{0xff})
	)

	var (
		iter = tdb.db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})
		sets  = map[string]uint64{}
		ndel  = 0
		batch = tdb.db.NewBatch()
	)

	for ok := iter.Last(); ok && !cn.close; ok = iter.Prev() {

		if bytes.Compare(iter.Key(), cutset) >= 0 {
			continue
		}

		if bytes.Compare(iter.Key(), offset) <= 0 {
			break
		}

		if len(iter.Value()) >= 2 {

			logMeta, err := kv2.ObjectMetaDecode(iter.Value())
			if err == nil && logMeta != nil {

				tdb.objectLogVersionSet(0, logMeta.Version, 0)

				if _, ok := sets[string(logMeta.Key)]; !ok {

					if ss := tdb.db.Get(keyEncode(nsKeyMeta, logMeta.Key), nil); ss.OK() {
						meta, err := kv2.ObjectMetaDecode(ss.Bytes())
						if err == nil && meta.Version > 0 && meta.Version != logMeta.Version {
							batch.Delete(iter.Key())
							ndel++
							continue
						}
					}

					sets[string(logMeta.Key)] = logMeta.Version
					continue
				}
			}
		}

		batch.Delete(iter.Key())
		ndel++

		if ndel >= 1000 {
			batch.Commit()
			batch = tdb.db.NewBatch()
			ndel = 0
			hlog.Printf("info", "table %s, log clean %d/%d", tdb.tableName, ndel, len(sets))
		}
	}

	if ndel > 0 {
		batch.Commit()
		hlog.Printf("info", "table %s, log clean %d/%d", tdb.tableName, ndel, len(sets))
	}

	iter.Release()

	return nil
}

func (cn *Conn) workerLocalSysStatusRefresh() error {

	tn := time.Now().Unix()
	if (cn.workerStatusRefreshed + workerStatusRefreshTime) > tn {
		return nil
	}

	if cn.perfStatus == nil {

		if ss := cn.dbSys.Get(nsSysApiStatus("node"), nil); ss.OK() {
			var perfStatus tsd.CycleFeed
			if kv2.StdProto.Decode(ss.Bytes(), &perfStatus) == nil && perfStatus.Unit == 10 {
				cn.perfStatus = &perfStatus
			}
		}

		if cn.perfStatus == nil {
			cn.perfStatus = tsd.NewCycleFeed(10)
		}

		for _, v := range []string{
			// API QPS
			PerfAPIReadKey, PerfAPIReadKeyRange, PerfAPIReadLogRange, PerfAPIWriteKey,
			// API BPS
			PerfAPIReadBytes, PerfAPIWriteBytes,
			// Storage QPS
			PerfStorReadKey, PerfStorReadKeyRange, PerfStorReadLogRange, PerfStorWriteKey,
			// Storage BPS
			PerfStorReadBytes, PerfStorWriteBytes,
		} {
			cn.perfStatus.Sync(v, 0, 0, tsd.ValueAttrSum)
		}

		sort.Slice(cn.perfStatus.Items, func(i, j int) bool {
			return strings.Compare(cn.perfStatus.Items[i].Name, cn.perfStatus.Items[j].Name) < 0
		})
	}

	if cn.sysStatus.Updated == 0 {
		cn.sysStatus.Caps = map[string]*kv2.SysCapacity{
			"cpu": {
				Use: int64(runtime.NumCPU()),
			},
		}
		cn.sysStatus.Addr = cn.opts.Server.Bind
		cn.sysStatus.Version = Version
		cn.sysStatus.Uptime = tn
	}

	cn.sysStatus.Updated = tn

	{
		vm, _ := ps_mem.VirtualMemory()
		cn.sysStatus.Caps["mem"] = &kv2.SysCapacity{
			Use: int64(vm.Used),
			Max: int64(vm.Total),
		}
	}

	if cn.workerStatusRefreshed == 0 || rand.Intn(10) == 0 {
		if st, err := ps_disk.Usage(cn.opts.Storage.DataDirectory); err == nil {
			cn.sysStatus.Caps["disk"] = &kv2.SysCapacity{
				Use: int64(st.Used),
				Max: int64(st.Total),
			}
		}
	}

	if bs, err := kv2.StdProto.Encode(cn.perfStatus); err == nil && len(bs) > 20 {
		cn.dbSys.Put(nsSysApiStatus("node"), bs, nil)
	}

	cn.workerStatusRefreshed = tn

	// debugPrint(cn.sysStatus)

	return nil
}
