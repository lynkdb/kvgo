// Copyright 2015 Eryx <evorui at gmail dot com>, All rights reserved.
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

package server

import (
	"errors"
	"fmt"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/hooto/hlog4g/hlog"
	ps_disk "github.com/shirou/gopsutil/v3/disk"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type jobManager struct {
	storemu sync.RWMutex
	stores  map[string]*ConfigStore
}

func (it *dbServer) jobSetup() error {

	if err := it.jobDatabaseListSetup(); err != nil {
		return err
	}

	go it.jobRun()

	return nil
}

func (it *dbServer) jobRun() {
	tr := time.NewTimer(1e9)
	for !it.close {
		select {
		case <-tr.C:
			if err := it.jobRefresh(); err != nil {
				hlog.Printf("warn", "job refresh err %s", err.Error())
			}
			tr.Reset(1e9)
		}
	}
}

func (it *dbServer) jobRefresh() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if err := it.jobDatabaseListSetup(); err != nil {
		hlog.Printf("info", "database list setup err %s", err.Error())
		return err
	}

	if err := it.jobStatusMergeSetup(); err != nil {
	}

	if err := it.jobStoreStatusRefresh(); err != nil {
	}

	if err := it.jobShardSplitSetup(); err != nil {
	}

	if err := it.jobReplicaRebalanceSetup(false); err != nil {
	}

	if err := it.jobShardMergeSetup(false); err != nil {
	}

	if err := it.jobDatabaseTransferSetup(); err != nil {
	}

	if err := it.jobDatabasePing(); err != nil {
	}

	return nil
}

func (it *dbServer) jobDatabaseListSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		offset = nsSysDatabaseSpec("")
		cutset = nsSysDatabaseSpec("zzzz")
	)

	for !it.close {
		req := kvapi.NewRangeRequest(offset, cutset).SetLimit(kDatabaseInstanceMax)

		rs := it.dbSystem.Range(req)
		if rs.NotFound() {
			break
		} else if !rs.OK() {
			return rs.Error()
		}

		for _, item := range rs.Items {

			var tbl kvapi.Database
			if err := item.JsonDecode(&tbl); err != nil {
				return err
			}

			it.dbMapMgr.syncDatabase(item.Meta, &tbl)

			if it.status.Uptime == 0 {
				hlog.Printf("info", "kvgo database %s (%d) started", tbl.Name, tbl.Id)
			}

			offset = item.Key
		}

		if !rs.NextResultSet {
			break
		}
	}

	offset = nsSysDatabaseMap("")
	cutset = nsSysDatabaseMap("zzzz")

	for !it.close {
		req := kvapi.NewRangeRequest(offset, cutset).SetLimit(kDatabaseInstanceMax)

		rs := it.dbSystem.Range(req)
		if rs.NotFound() {
			break
		} else if !rs.OK() {
			return rs.Error()
		}

		for _, item := range rs.Items {

			var tm kvapi.DatabaseMap
			if err := item.JsonDecode(&tm); err != nil {
				return err
			}

			ptm := it.dbMapMgr.getById(tm.Id)
			if ptm == nil {
				continue
			}

			ptm.syncMap(item.Meta, &tm)

			offset = item.Key
		}

		if !rs.NextResultSet {
			break
		}
	}

	it.dbMapMgr.initIter(func(tm *dbMap) {
		if err := it._jobDatabaseMapSetup(tm); err != nil {
			hlog.Printf("info", "database setup err %s", err.Error())
		}
	})

	return nil
}

func (it *dbServer) _jobDatabaseMapSetup(tm *dbMap) error {

	it.jobSetupMut.Lock()
	defer it.jobSetupMut.Unlock()

	if tm.data.Name == sysDatabaseName {
		return nil
	}

	if len(it.cfg.Storage.Stores) == 0 ||
		it.storeMgr.ok() == 0 {
		return errors.New("storage stores not setup")
	}

	var (
		tn  = timesec()
		chg = false
	)

	if tm.mapData == nil {

		r := kvapi.NewReadRequest(nsSysDatabaseMap(tm.data.Id))
		r.Database = sysDatabaseName

		if rs, err := it.api.Read(nil, r); err != nil {
			return err
		} else if rs.OK() {
			item := rs.Item()
			if item == nil {
				return errors.New("no data found")
			}

			var m kvapi.DatabaseMap
			if err := item.JsonDecode(&m); err != nil {
				return err
			}

			tm.mapMeta = item.Meta
			tm.mapData = &m

		} else if rs.NotFound() {

			tm.mapMeta = &kvapi.Meta{}
			tm.mapData = &kvapi.DatabaseMap{
				Id:      tm.data.Id,
				Version: 1,
			}
			chg = true

		} else {
			return rs.Error()
		}
	}

	if tm.mapData == nil {
		return nil
	}

	if len(tm.mapData.Shards) == 0 {
		if !chg {
			tm.mapData.Version += 1
		}
		tm.mapData.Shards, chg = []*kvapi.DatabaseMap_Shard{
			{
				Id:      tm.nextIncr(),
				Version: tm.mapData.Version,
				Action:  kShardSetup_In,
				Updated: timesec(),
			},
		}, true
	}

	if tm.data.ReplicaNum < minReplicaCap {
		tm.data.ReplicaNum = minReplicaCap
	} else if tm.data.ReplicaNum > maxReplicaCap {
		tm.data.ReplicaNum = maxReplicaCap
	}

	for _, shard := range tm.mapData.Shards {

		for len(shard.Replicas) < int(tm.data.ReplicaNum) {

			allocStores := map[uint64]bool{}
			for _, rep := range shard.Replicas {
				allocStores[rep.StoreId] = true
			}

			fitStore := it.storeMgr.lookupFitStore(allocStores)
			if fitStore == nil {
				break
			}

			if !chg {
				tm.mapData.Version += 1
			}
			rep := &kvapi.DatabaseMap_Replica{
				Id:      tm.nextIncr(),
				StoreId: fitStore.Id,
				Action:  kReplicaSetup_In,
			}
			shard.Replicas, chg = append(shard.Replicas, rep), true

			// tm.syncStatusReplica(rep.Id, kReplicaStatus_In)
		}
	}

	for i, shard := range tm.mapData.Shards {

		ins := []*kvapi.DatabaseMap_Replica{}

		for _, rep := range shard.Replicas {

			if kvapi.AttrAllow(rep.Action, kReplicaSetup_Out) &&
				kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
				it.auditLogger.Put("dbmap", "shard %d, rep %d, removed", shard.Id, rep.Id)
				continue
			}

			ins = append(ins, rep)
		}

		if len(ins) < len(shard.Replicas) {
			it.auditLogger.Put("dbmap", "shard %d, replica-cap %d to %d", shard.Id, len(shard.Replicas), len(ins))
			shard.Updated = timesec()
			shard.Replicas, chg = ins, true
		}

		if !chg &&
			kvapi.AttrAllow(shard.Action, kShardSetup_Out) &&
			shard.Updated+kShardOutRemove_JobIntervalSeconds < tn {
			tm.mapData.Version += 1
			tm.mapData.Updated = timesec()
			tm.mapData.Shards = append(tm.mapData.Shards[:i], tm.mapData.Shards[i+1:]...)
			chg = true
			it.auditLogger.Put("dbmap", "shard %d, out-removed", shard.Id)
			break
		}
	}

	if chg {
		wr := kvapi.NewWriteRequest(nsSysDatabaseMap(tm.data.Id), jsonEncode(tm.mapData))
		wr.Database = sysDatabaseName
		if tm.mapMeta == nil {
			wr.CreateOnly = true
		} else {
			wr.PrevVersion = tm.mapMeta.Version
		}
		rs, err := it.api.Write(nil, wr)
		if err != nil {
			return err
		}
		if !rs.OK() {
			return rs.Error()
		}

		// TODO update meta.version
	}

	var err error

	for _, shard := range tm.mapData.Shards {

		for _, rep := range shard.Replicas {

			store := tm.getStore(rep.StoreId)
			if store != nil {
				continue
			}

			cfg := it.storeMgr.getConfig(rep.StoreId)
			if cfg == nil {
				continue
			}

			opts := it.cfg.cloneStorageOptions()
			opts.DataDirectory = fmt.Sprintf("%s/data/%s_%s", cfg.Mountpoint, tm.data.Id, tm.data.Engine)

			store, err = storage.Open(tm.data.Engine, opts)
			if err != nil {
				return err
			}

			tm.syncStore(rep.StoreId, store)

			hlog.Printf("info", "store setup %s ok", opts.DataDirectory)
		}
	}

	return nil
}

func (it *dbServer) jobStatusMergeSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	storeReplicaBounds := map[uint64]int64{}

	tmStatusRefresh := func(tm *dbMap) {

		if tm.mapData == nil {
			return
		}

		for _, shard := range tm.mapData.Shards {

			shardVersion := shard.Version

			//
			for _, rep := range shard.Replicas {

				if _, ok := storeReplicaBounds[rep.StoreId]; !ok {
					storeReplicaBounds[rep.StoreId] = 1
				} else {
					storeReplicaBounds[rep.StoreId] += 1
				}

				var (
					repInst = tm.replica(rep.Id)
					// repStatus = tm.statusReplica(rep.Id)
					actions = map[uint64]int{}
				)

				if repInst == nil {
					continue
				}

				if tm.data.ReplicaNum == 1 {
					actions[kReplicaStatus_Ready] = 1

				} else if len(repInst.localStatus.iterPulls) > 0 {
					//
					for _, v := range repInst.localStatus.iterPulls {

						if v.mapVersion != shardVersion {
							continue
						}

						if (kvapi.AttrAllow(rep.Action, kReplicaSetup_In) ||
							kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveIn)) &&
							kvapi.AttrAllow(uint64(v.value), kReplicaStatus_Ready) {
							actions[kReplicaStatus_Ready] += 1
						}
					}

					// TODO
					if kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
						actions[kReplicaStatus_Ready] += 1
					}
				}

				if kvapi.AttrAllow(shard.Action, kShardSetup_In) {

					if len(actions) == 0 {
						// testPrintf("shard %d, rep %d, actions %v", shard.Id, rep.Id, actions)
					}

					if actions[kReplicaStatus_Ready]*2 > int(tm.data.ReplicaNum) {

						repInst.localStatus.action.attr |= kReplicaStatus_Ready
						repInst.localStatus.action.mapVersion = shardVersion
						repInst.localStatus.action.updated = timesec()

						tm.setReplicaStatus(rep.Id, func(s *kvapi.DatabaseMapStatus_Replica) {
							s.Action |= kReplicaStatus_Ready
							s.MapVersion = shardVersion
							s.Used = repInst.localStatus.storageUsed.value
							s.Updated = timesec()
							s.LogVersion, _ = repInst.logSync(0, 0)
						})

					} else if kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
						//
					}
				}
			}
		}
	}

	it.dbMapMgr.iter(func(tm *dbMap) {
		tmStatusRefresh(tm)
	})

	it.storeMgr.iterSetStatus(func(s *kvapi.SysStoreStatus) {
		if num, ok := storeReplicaBounds[s.Id]; ok {
			s.ReplicaBounds = num
		} else {
			s.ReplicaBounds = 0
		}
	})

	return nil
}

func (it *dbServer) jobOnce() {
	go it.jobDatabaseAutoClean()
	go it.jobDatabaseLogPull(false)
}

func (it *dbServer) jobDatabaseAutoClean() {

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		lastCleanTime = time.Now().Unix()
		tr            = time.NewTimer(1e9)
	)
	defer tr.Stop()

	for !it.close {
		tr.Reset(1e9)
		t := <-tr.C
		if lastCleanTime+5 > t.Unix() {
			continue
		}

		it.dbMapMgr.iter(func(tm *dbMap) {
			for _, rep := range tm.replicas {
				if err := rep._jobCleanTTL(); err != nil {
					hlog.Printf("error", "kvgo job clean ttl err %s", err.Error())
				}
				if err := rep._jobCleanLog(); err != nil {
					hlog.Printf("error", "kvgo job clean log err %s", err.Error())
				}
			}
		})

		lastCleanTime = time.Now().Unix()
	}
}

func (it *dbServer) jobDatabaseLogPull(force bool) {

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		lastCleanTime = int64(0)
		tr            = time.NewTimer(1e9)
	)
	defer tr.Stop()

	for !it.close {
		tr.Reset(1e9)
		t := <-tr.C
		if !force && lastCleanTime+5 > t.Unix() {
			continue
		}
		it.dbMapMgr.iter(func(tm *dbMap) {

			if tm.data.ReplicaNum <= 1 {
				return
			}

			for i := 0; i < len(tm.mapData.Shards); i++ {

				var (
					shard    = tm.mapData.Shards[i]
					lowerKey = bytesClone(shard.LowerKey)
					upperKey = []byte{0xff}
				)

				if i+1 < len(tm.mapData.Shards) {
					upperKey = bytesClone(tm.mapData.Shards[i+1].LowerKey)
				}

				for _, src := range shard.Replicas {

					if !kvapi.AttrAllow(src.Action, kReplicaSetup_In) {
						continue
					}

					srcInst := tm.replica(src.Id)
					if srcInst == nil {
						continue
					}

					for _, dst := range shard.Replicas {
						if src.Id == dst.Id {
							continue
						}

						if !kvapi.AttrAllow(dst.Action, kReplicaSetup_In) &&
							!kvapi.AttrAllow(dst.Action, kReplicaSetup_MoveIn) {
							continue
						}

						dstInst := tm.replica(dst.Id)
						if dstInst == nil {
							continue
						}

						err := dstInst._jobLogPull(tm, shard, lowerKey, upperKey, srcInst, dstInst.localStatus.pull(src.Id))
						if err != nil {
							hlog.Printf("info", "database %s, shard %d, rep dst/src %d/%d, sync-pull err %s",
								tm.data.Name, shard.Id, dst.Id, src.Id, err.Error())
						}
					}
				}
			}
		})

		lastCleanTime = time.Now().Unix()
		if force {
			break
		}
	}
}

func (it *dbServer) mapFlush(tm *dbMap, mapData *kvapi.DatabaseMap, prevVersion uint64) error {

	if prevVersion != tm.mapMeta.Version {
		return errors.New("meta version confict")
	}

	if mapData.Version < tm.mapData.Version {
		return errors.New("map-version confict")
	}

	if mapData.Version == tm.mapData.Version {
		mapData.Version += 1
	}

	if mapData.IncrId < tm.mapData.IncrId {
		mapData.IncrId = tm.mapData.IncrId
	}

	wr := kvapi.NewWriteRequest(nsSysDatabaseMap(tm.data.Id), jsonEncode(mapData))
	wr.Database = sysDatabaseName
	wr.PrevVersion = prevVersion

	rs, err := it.api.Write(nil, wr)
	if err != nil {
		hlog.Printf("error", "mapdata flush fail %s", err.Error())
		testPrintf("mapdata flush fail %s", err.Error())
		return err
	}
	if !rs.OK() {
		return rs.Error()
	}

	item := rs.Item()
	if item == nil {
		return errors.New("unspec error")
	}

	tm.mapMeta = item.Meta
	tm.mapData = mapData

	return nil
}

func (it *dbServer) _job_replicaRemoveSetup(tm *dbMap, mapData *kvapi.DatabaseMap) (chg bool) {

	for _, shard := range mapData.Shards {

		//
		if len(shard.Replicas) <= int(tm.data.ReplicaNum) {
			continue
		}

		// step 2
		for i, rep := range shard.Replicas {
			if kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
				continue
			}
			repInst := tm.replica(rep.Id)
			if repInst == nil {
				continue
			}
			if !kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Remove) {
				continue
			}

			shard.Replicas = append(shard.Replicas[:i], shard.Replicas[i+1:]...)
			testPrintf("shard %d, replica %d, removed", shard.Id, rep.Id)

			it.auditLogger.Put("replica", "shard %d, rep %d, removed", shard.Id, rep.Id)

			return true
		}

		// step 1
		ok := 0
		for _, rep := range shard.Replicas {
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
				continue
			}
			repInst := tm.replica(rep.Id)
			if repInst == nil {
				continue
			}
			if !kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				continue
			}
			ok += 1
		}
		if ok < int(tm.data.ReplicaNum) {
			continue
		}

		for _, rep := range shard.Replicas {
			if kvapi.AttrAllow(rep.Action, kReplicaSetup_Out) &&
				!kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
				rep.Action |= kReplicaSetup_Remove
				it.auditLogger.Put("replica", "shard %d, rep %d, try-remove", shard.Id, rep.Id)
				return true
			}
		}
	}

	return false
}

func (it *dbServer) jobKeyMapSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	it.jobSetupMut.Lock()
	defer it.jobSetupMut.Unlock()

	dbAction := func(tm *dbMap) {

		var (
			prevVersion = tm.mapMeta.Version
			mapData     kvapi.DatabaseMap
			chg         bool
		)

		if err := objectClone(tm.mapData, &mapData); err != nil {
			return
		}

		if it._job_replicaRemoveSetup(tm, &mapData) {
			chg = true
		}

		if chg {
			err := it.mapFlush(tm, &mapData, prevVersion)
			if err != nil {
			}
		}
	}

	it.dbMapMgr.iter(dbAction)

	return nil
}

func (it *dbServer) jobStoreStatusRefresh() error {

	const name = "jobStoreStatusRefresh"

	var (
		tn      = timesec()
		ver, ok = it.mum.Load(name)
	)

	if ok && ver.(int64)+jobStoreStatusRefreshIntervalSecond > tn {
		return nil
	}
	it.mum.Store(name, tn)

	defer it.storeMgr.updateStatusVersion()

	for _, vol := range it.cfg.Storage.Stores {

		if vol.StoreId == 0 {
			continue
		}

		st, err := ps_disk.Usage(vol.Mountpoint)
		if err != nil {
			hlog.Printf("warn", "job store status refresh err %s", err.Error())
			continue
		}

		var (
			used  = int64(st.Used) / (1 << 20)
			total = int64(st.Total) / (1 << 20)

			logUsed  int64 = -1
			metaUsed int64 = -1
			dataUsed int64 = -1
			ttlUsed  int64 = -1
		)

		if store := it.storeMgr.store(vol.StoreId); store != nil {

			sizeIter := func(ns byte) int64 {
				if rs, err := store.SizeOf([]*storage.IterOptions{
					{
						LowerKey: keyEncode(ns, []byte{0x00}),
						UpperKey: keyEncode(ns, []byte{0xff}),
					},
				}); err == nil {
					return rs[0] / (1 << 20)
				}
				return -1
			}

			logUsed = sizeIter(nsKeyLog)
			metaUsed = sizeIter(nsKeyMeta)
			dataUsed = sizeIter(nsKeyData)
			ttlUsed = sizeIter(nsKeyTtl)
		}

		it.storeMgr.setStatus(vol.UniId, vol.StoreId, func(s *kvapi.SysStoreStatus) {
			s.CapacityUsed = used
			s.CapacityFree = total - used
			s.Updated = timesec()
			s.Options["fstype"] = st.Fstype
			if logUsed >= 0 {
				s.LogUsed = logUsed
			}
			if metaUsed >= 0 {
				s.MetaUsed = metaUsed
			}
			if dataUsed >= 0 {
				s.DataUsed = dataUsed
			}
			if ttlUsed >= 0 {
				s.TtlUsed = ttlUsed
			}
		})

		// testPrintf("store %s, used %d GB", vol.Mountpoint, used/(1<<10))
	}

	return nil
}

func (it *dbServer) jobDatabasePing() error {

	if it.close {
		return nil
	}

	if timesec()-it.status.Uptime > 60 &&
		mrand.Intn(3) > 0 {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	it.dbMapMgr.initIter(func(tm *dbMap) {

		if mrand.Intn(2) == 0 {

			wr := kvapi.NewWriteRequest(jobDatabasePing_MagicKey, randBytes(20))
			wr.Database = tm.data.Name

			// wr.SetTTL(60e3)

			rs, err := it.api.Write(nil, wr)
			if err != nil {
				return
			}
			if !rs.OK() {
				return
			}

		} else {

			wr := kvapi.NewDeleteRequest(jobDatabasePing_MagicKey)
			wr.Database = tm.data.Name

			rs, err := it.api.Delete(nil, wr)
			if err != nil {
				return
			}
			if !rs.OK() {
				return
			}
		}
	})

	return nil
}
