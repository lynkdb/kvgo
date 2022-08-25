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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hooto/hlog4g/hlog"
	ps_cpu "github.com/shirou/gopsutil/v3/cpu"
	ps_disk "github.com/shirou/gopsutil/v3/disk"
	ps_mem "github.com/shirou/gopsutil/v3/mem"
	ps_net "github.com/shirou/gopsutil/v3/net"

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
		num = 0
		ok  bool
	)
	defer iter.Release()

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
			err := batch.Commit()
			if err != nil {
				hlog.Printf("debug", "table %s, ttl clean %d, err %v",
					dt.tableName, num, err)
				return err
			}
			num += batch.Len()
			batch.Reset()
		}
	}

	if batch.Len() > 0 {
		err := batch.Commit()
		if err != nil {
			hlog.Printf("debug", "table %s, ttl clean %d, err %v",
				dt.tableName, num, err)
			return err
		}
		num += batch.Len()
	}

	if num > 0 {
		hlog.Printf("debug", "table %s, ttl clean %d", dt.tableName, num)
	}

	dt.expiredSync(tn + 1000)

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
	nsItems := []struct {
		name string
		ns   uint8
	}{
		{"sys", nsKeySys},
		{"meta", nsKeyMeta},
		{"data", nsKeyData},
		{"log", nsKeyLog},
		{"ttl", nsKeyTtl},
	}
	nsSysItems := []struct {
		name   string
		prefix []byte
	}{
		{"incr_id", keySysIncrCutset("")},
	}

	for _, t := range cn.tables {

		if err := cn.workerLocalLogCleanTable(t); err != nil {
			hlog.Printf("warn", "worker log clean table %s, err %s",
				t.tableName, err.Error())
		}

		// db size
		siz, err := t.db.SizeOf(rgS)
		if err != nil {
			hlog.Printf("warn", "get db size error %s", err.Error())
			continue
		}
		if len(siz) < 1 {
			continue
		}

		tableStatus := kv2.TableStatus{
			Name:   t.tableName,
			KeyNum: 0,
			DbSize: uint64(siz[0]),
			// Options: map[string]int64{},
			States: map[string]string{},
		}

		for _, tp := range nsItems {
			var (
				kn = int64(0)
				rg = &kv2.StorageIteratorRange{
					Start: keyEncode(tp.ns, []byte{}),
					Limit: keyEncode(tp.ns, []byte{0xff}),
				}
			)
			iter := t.db.NewIterator(rg)
			for ok := iter.First(); ok; ok = iter.Next() {
				kn++
				/**
				if tp.name == "sys" {
					tableStatus.States["sys_"+string(iter.Key()[1:])] = string(iter.Value())
				}
				*/
			}
			iter.Release()

			if kn == 0 {
				continue
			}

			if tableStatus.States["ns_"+tp.name] == "" {
				tableStatus.States["ns_"+tp.name] = fmt.Sprintf("keys:%d", kn)
			} else {
				tableStatus.States["ns_"+tp.name] += fmt.Sprintf(",keys:%d", kn)
			}

			if tp.name == "meta" || tp.name == "data" {
				tableStatus.KeyNum += uint64(kn)
			}

			if siz, err := t.db.SizeOf([]*kv2.StorageIteratorRange{rg}); err == nil || len(siz) > 0 {
				if tableStatus.States["ns_"+tp.name] == "" {
					tableStatus.States["ns_"+tp.name] = fmt.Sprintf("size:%d", siz[0])
				} else {
					tableStatus.States["ns_"+tp.name] += fmt.Sprintf(",size:%d", siz[0])
				}
			}
		}

		for _, tp := range nsSysItems {

			var (
				rg = &kv2.StorageIteratorRange{
					Start: tp.prefix,
					Limit: append(tp.prefix, []byte{0xff}...),
				}
				iter = t.db.NewIterator(rg)
			)

			for ok := iter.First(); ok; ok = iter.Next() {

				if bytes.Compare(iter.Key(), rg.Start) <= 0 {
					continue
				}

				if bytes.Compare(iter.Key(), rg.Limit) > 0 {
					break
				}

				val, err := strconv.ParseInt(string(iter.Value()), 10, 64)
				if err != nil || val == 0 {
					continue
				}

				key := bytes.TrimPrefix(iter.Key(), rg.Start)
				if len(key) == 0 {
					continue
				}

				if tableStatus.States[tp.name] == "" {
					tableStatus.States[tp.name] = fmt.Sprintf("%s:%d", string(key), val)
				} else {
					tableStatus.States[tp.name] += fmt.Sprintf(",%s:%d", string(key), val)
				}
			}

			iter.Release()
		}

		// log-id
		if ss := t.db.Get(keySysLogCutset, nil); ss.OK() {
			if logid, err := strconv.ParseInt(ss.String(), 10, 64); err == nil {
				tableStatus.States["log_id"] = fmt.Sprintf("%d", logid)
			}
		}

		for k2, v2 := range t.logPullOffsets {
			if tableStatus.States["log_sync_pull"] == "" {
				tableStatus.States["log_sync_pull"] = fmt.Sprintf("from:%s,log:%d", k2, v2.LogOffset)
			} else {
				tableStatus.States["log_sync_pull"] = fmt.Sprintf("\nfrom:%s,log:%d", k2, v2.LogOffset)
			}
		}

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

	lockKey := fmt.Sprintf("task/table/sync/log-pull/addr:%s/table:%s:%s", hp.Addr, tm.From, tm.To)
	if _, ok := cn.taskLocks.LoadOrStore(lockKey, "true"); ok {
		return nil
	}
	defer cn.taskLocks.Delete(lockKey)

	var (
		offset = tdb.logPullOffsetFlush(hp.Addr, tm.From, tm.To, nil, false)
		retry  = 0
		// mttl   = uint64(3600) * 1000
	)

	if offset == nil {
		return errors.New("db error")
	}

	conn, err := clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, false)
	if err != nil {
		return err
	}

	if len(offset.DataKeyOffset) == 0 {
		offset.DataKeyOffset = []byte{0x00}
	}
	if len(offset.DataKeyCutset) == 0 {
		offset.DataKeyCutset = bytes.Repeat([]byte{0xff}, 32)
	}

	if len(offset.MetaKeyOffset) == 0 {
		offset.MetaKeyOffset = []byte{0x00}
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

			hlog.Printf("info", "sync log-pull from %s/%s, offset (%d) %s, try ...",
				hp.Addr, tdb.tableName, len(req.KeyOffset), string(req.KeyOffset))

			ctx, fc := context.WithTimeout(context.Background(), time.Second*100)
			rep, err := kv2.NewInternalClient(conn).LogSync(ctx, req)
			fc()

			if err != nil {
				retry++

				hlog.SlotPrint(60, "warn", "sync log-pull from %s/%s, err %s, retry(%d) ...",
					hp.Addr, tdb.tableName, err.Error(), retry)

				time.Sleep(1e9)
				conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
				continue
			}
			retry = 0

			hlog.Printf("info", "sync log-pull from %s/%s, len %d, offset %s",
				hp.Addr, tdb.tableName, len(rep.Logs), string(req.KeyOffset))

			if cn.close {
				break
			}

			if len(rep.Logs) == 0 {
				break
			}

			statsScan += len(rep.Logs)

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			tlog := timems()

			for _, v := range rep.Logs {

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				tl := tlog - int64(v.Updated)
				if tl < 0 {
					tl -= tl
				}

				if cn.monitor != nil {

					cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(int64(len(v.Key)))

					cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(tl)
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && meta.Version >= v.Version { // && (meta.Updated+mttl) >= v.Updated) {
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

				tlog := timems()

				for _, item := range rep2.Items {

					tl := tlog - int64(item.Meta.Updated)
					if tl < 0 {
						tl -= tl
					}

					siz := len(item.Meta.Key) + len(item.Data.Value)

					if cn.monitor != nil {
						cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(int64(siz))

						cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(tl)
					}

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

					hlog.Printf("info", "sync log-pull from %s/%s to local/%s, data keys sync/scan %d/%d, log offset %d, key offset %v",
						hp.Addr, tm.From, tm.To, statsSync, statsScan, item.Meta.Version, string(item.Meta.Key))
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys

				hlog.SlotPrint(600, "info", "sync log-pull from %s/%s to local/%s, data keys sync/scan %d/%d, log offset %d, key offset %v",
					hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.DataKeyOffset))
			}

			if len(rep.Logs) > 0 {
				offset.DataKeyOffset = bytesClone(rep.Logs[len(rep.Logs)-1].Key)
			}

			tdb.logPullOffsetFlush(hp.Addr, tm.From, tm.To, offset, true)
			hlog.SlotPrint(600, "info", "sync log-pull from %s/%s to local/%s, data keys sync/scan %d/%d, log offset %d, key offset %v",
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

				hlog.SlotPrint(600, "warn", "sync log-pull from %s/%s, err %s, retry(%d) ...",
					hp.Addr, tdb.tableName, err.Error(), retry)

				time.Sleep(1e9)
				conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
				continue
			}
			retry = 0

			hlog.Printf("info", "sync log-pull from %s/%s, len %d, offset %s",
				hp.Addr, tdb.tableName, len(rep.Logs), string(req.KeyOffset))

			if cn.close {
				break
			}

			if len(rep.Logs) == 0 {
				break
			}

			statsScan += len(rep.Logs)

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			tlog := timems()

			for _, v := range rep.Logs {

				tl := tlog - int64(v.Updated)
				if tl < 0 {
					tl -= tl
				}

				if cn.monitor != nil {
					cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(int64(len(v.Key)))

					cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(tl)
				}

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && meta.Version >= v.Version { // && (meta.Updated+mttl) >= v.Updated) {
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

				tlog := timems()

				for _, item := range rep2.Items {

					tl := tlog - int64(item.Meta.Updated)
					if tl < 0 {
						tl -= tl
					}

					siz := len(item.Meta.Key) + len(item.Data.Value)

					if cn.monitor != nil {
						cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(int64(siz))

						cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(tl)
					}

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

					hlog.Printf("info", "sync log-pull from %s/%s to local/%s, meta keys sync/scan %d/%d, log offset %d, key offset %v",
						hp.Addr, tm.From, tm.To, statsSync, statsScan, item.Meta.Version, string(item.Meta.Key))
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys

				hlog.SlotPrint(600, "info", "sync log-pull from %s/%s to local/%s, meta keys sync/scan %d/%d, log offset %d, key offset %v",
					hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset, string(offset.MetaKeyOffset))
			}

			if len(rep.Logs) > 0 {
				offset.MetaKeyOffset = bytesClone(rep.Logs[len(rep.Logs)-1].Key)
			}

			tdb.logPullOffsetFlush(hp.Addr, tm.From, tm.To, offset, true)
			hlog.SlotPrint(600, "info", "sync log-pull from %s/%s to local/%s, meta keys sync/scan %d/%d, log offset %d, key offset %v",
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

			hlog.SlotPrint(600, "warn", "sync log-pull from %s/%s, err %s, retry(%d) ...",
				hp.Addr, tdb.tableName, err.Error(), retry)

			time.Sleep(1e9)
			conn, err = clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, true)
			continue
		}
		retry = 0

		hlog.Printf("debug", "sync log-pull from %s/%s, len %d, offset %d",
			hp.Addr, tdb.tableName, len(rep.Logs), req.LogOffset)

		if cn.close {
			break
		}

		if len(rep.Logs) > 0 {

			req2 := &kv2.LogSyncRequest{
				ServerId:  cn.opts.Server.ID,
				Addr:      cn.opts.Server.Bind,
				TableName: tm.From,
			}

			tlog := timems()

			for _, v := range rep.Logs {

				tl := tlog - int64(v.Updated)
				if tl < 0 {
					tl -= tl
				}

				// hlog.Printf("info", "logs key %s, updated %d", string(v.Key), v.Updated)

				if cn.monitor != nil {
					cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(int64(len(v.Key)))

					cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
						"TableLog": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
					}).Add(tl)
				}

				nsKey := uint8(0)
				if kv2.AttrAllow(v.Attrs, kv2.ObjectMetaAttrMetaOff) {
					nsKey = nsKeyData
				} else {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, v.Key), nil)
				if ss.OK() {
					meta, err := kv2.ObjectMetaDecode(ss.Bytes())
					if err == nil && meta.Version >= v.Version { // && (meta.Updated+mttl) >= v.Updated) {
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

				tlog := timems()

				for _, item := range rep2.Items {

					tl := tlog - int64(item.Meta.Updated)
					if tl < 0 {
						tl -= tl
					}

					siz := len(item.Meta.Key) + len(item.Data.Value)

					if cn.monitor != nil {
						cn.monitor.Metric(MetricLogSyncCall).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(int64(siz))

						cn.monitor.Metric(MetricLogSyncLatency).With(map[string]string{
							"TableKey": fmt.Sprintf("%s/%s", hp.Addr, tm.From),
						}).Add(tl)
					}

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

					hlog.Printf("info", "sync log-pull from %s/%s to local/%s, keys sync/scan %d/%d, log offset %d, key offset %v",
						hp.Addr, tm.From, tm.To, statsSync, statsScan, item.Meta.Version, string(item.Meta.Key))
				}

				statsSync += len(rep2.Items)
				req2.Keys = rep2.NextKeys
			}

			if len(rep.Logs) > 0 {
				offset.LogOffset = rep.Logs[len(rep.Logs)-1].Version
			}

			hlog.SlotPrint(600, "info", "sync log-pull from %s/%s to local/%s, log keys sync/scan %d/%d, log offset %d",
				hp.Addr, tm.From, tm.To, statsSync, statsScan, offset.LogOffset)
		}

		if len(rep.Logs) == 0 {
			time.Sleep(3e9)
		}

		if offset.LogOffset > req.LogOffset {
			req.LogOffset = offset.LogOffset
			tdb.logPullOffsetFlush(hp.Addr, tm.From, tm.To, offset, true)
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

var (
	diskDevSet []ps_disk.PartitionStat
)

func (cn *Conn) workerLocalSysStatusRefresh() error {

	tn := time.Now().Unix()
	if (cn.workerStatusRefreshed + workerStatusRefreshTime) > tn {
		return nil
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

		if cn.monitor != nil {

			cn.monitor.Metric(MetricSystem).With(map[string]string{
				"Memory": "Used",
			}).Add(int64(vm.Used))
			cn.monitor.Metric(MetricSystem).With(map[string]string{
				"Memory": "Cached",
			}).Add(int64(vm.Cached))

			if cio, _ := ps_cpu.Percent(0, false); len(cio) > 0 {
				cn.monitor.Metric(MetricSystem).With(map[string]string{
					"CPU": "Percent",
				}).Add(int64(cio[0]))
			}

			// Network
			if nio, _ := ps_net.IOCounters(false); len(nio) > 0 {
				cn.monitor.Metric(MetricSystem).With(map[string]string{
					"Net": "Recv",
				}).Set(1, int64(nio[0].BytesRecv))
				cn.monitor.Metric(MetricSystem).With(map[string]string{
					"Net": "Sent",
				}).Set(1, int64(nio[0].BytesRecv))
			}

			// Storage
			if diskDevSet == nil {
				diskDevSet, _ = ps_disk.Partitions(false)
			}
			diskFilter := func(pls []ps_disk.PartitionStat, path string) string {
				path = filepath.Clean(path)
				for {
					for _, v := range pls {
						if path == v.Mountpoint {
							if strings.HasPrefix(v.Device, "/dev/") {
								return v.Device[5:]
							}
							return ""
						}
					}
					if i := strings.LastIndex(path, "/"); i > 0 {
						path = path[:i]
					} else if len(path) > 1 && path[0] == '/' {
						path = "/"
					} else {
						break
					}
				}
				return ""
			}
			if devName := diskFilter(diskDevSet, cn.opts.Storage.DataDirectory); devName != "" {
				if diom, err := ps_disk.IOCounters(devName); err == nil {
					if dio, ok := diom[devName]; ok {
						cn.monitor.Metric(MetricSystem).With(map[string]string{
							"Disk": "Read",
						}).Set(int64(dio.ReadCount), int64(dio.ReadBytes))
						cn.monitor.Metric(MetricSystem).With(map[string]string{
							"Disk": "Write",
						}).Set(int64(dio.WriteCount), int64(dio.WriteBytes))
					}
				}
			}
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

	cn.workerStatusRefreshed = tn

	// debugPrint(cn.sysStatus)

	return nil
}
