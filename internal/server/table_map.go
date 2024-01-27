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
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type dbMapMgr struct {
	mu sync.RWMutex

	cfg *Config

	storeMgr *storeManager

	databases    map[string]*dbMap
	arrDatabases []*dbMap

	// database name -> id
	indexes map[string]string
}

type dbMapSelectShard struct {
	mapVersion uint64

	shardId uint64

	keys [][]byte

	lowerKey []byte
	upperKey []byte

	replicas   []*dbReplica
	replicaNum int
}

type dbMapShardStats struct {
	usageAvg int64

	lastSetUpdated int64
}

type dbMap struct {
	mu sync.RWMutex

	cfg *Config

	meta *kvapi.Meta
	data *kvapi.Database

	mapMeta *kvapi.Meta
	mapData *kvapi.DatabaseMap

	storeMgr *storeManager

	repMut      sync.RWMutex
	replicas    map[uint64]*dbReplica
	arrReplicas []*dbReplica
	rmReplicas  map[uint64]int64

	stmut  sync.RWMutex
	status kvapi.DatabaseMapStatus
}

func (it *dbMap) replica(id uint64) *dbReplica {
	it.repMut.Lock()
	defer it.repMut.Unlock()
	if rep, ok := it.replicas[id]; ok {
		return rep
	}
	return nil
}

func (it *dbMap) tryRemoveReplica(repId uint64) {
	it.repMut.Lock()
	defer it.repMut.Unlock()
	// it.rmReplicas[repId] = 1
}

func (it *dbMap) tryInitReplica(shard *kvapi.DatabaseMap_Shard, rep *kvapi.DatabaseMap_Replica) *dbReplica {
	it.repMut.Lock()
	defer it.repMut.Unlock()

	if _, ok := it.rmReplicas[rep.Id]; ok {
		return nil
	}

	store := it.storeMgr.store(rep.StoreId)
	if store == nil {
		return nil
	}

	trep, ok := it.replicas[rep.Id]
	if !ok {
		trep, err = NewDatabase(store, it.data.Id, it.data.Name, shard.Id, rep.Id, it.cfg)
		if err != nil {
			return nil
		}

		it.replicas[rep.Id] = trep
		it.arrReplicas = append(it.arrReplicas, trep)
	}
	return trep
}

func (it *dbMap) iterStatusDisplay(fn func(
	shard *kvapi.DatabaseMap_Shard,
	shardStatus string,
	replicaStatus []string,
)) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	for _, shard := range it.mapData.Shards {
		as := []string{}
		for _, rep := range shard.Replicas {
			if repInst := it.replica(rep.Id); repInst != nil {
				as = append(as, fmt.Sprintf("#%d %s %s %dM %ds", rep.Id,
					ReplicaActionDisplay(rep.Action), ReplicaActionDisplay(repInst.localStatus.action.attr),
					repInst.localStatus.storageUsed.value,
					timesec()-repInst.localStatus.storageUsed.updated,
				))
			} else {
				as = append(as, fmt.Sprintf("#%d %s %s %d -1", rep.Id,
					ReplicaActionDisplay(rep.Action), ReplicaActionDisplay(0), 0))
			}
		}
		fn(shard, ShardActionDisplay(shard.Action), as)
	}
}

func newDatabaseMapMgr(cfg *Config, storeMgr *storeManager) *dbMapMgr {
	if cfg == nil {
		panic("no config setup")
	}
	if storeMgr == nil {
		panic("no store setup")
	}
	cfg.Reset()
	mgr := &dbMapMgr{
		cfg:       cfg,
		storeMgr:  storeMgr,
		databases: map[string]*dbMap{},
		indexes:   map[string]string{},
	}
	return mgr
}

func (it *dbMapMgr) getByName(name string) *dbMap {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if id, ok := it.indexes[name]; ok {
		if t, ok := it.databases[id]; ok {
			return t
		}
	}
	return nil
}

func (it *dbMapMgr) getById(id string) *dbMap {
	it.mu.Lock()
	defer it.mu.Unlock()
	if pt, ok := it.databases[id]; ok {
		return pt
	}
	return nil
}

func (it *dbMapMgr) initIter(fn func(tbl *dbMap)) {
	for _, tm := range it.arrDatabases {
		if tm == nil || tm.meta == nil || tm.data == nil || tm.data.ReplicaNum < 1 {
			continue
		}
		fn(tm)
	}
}

func (it *dbMapMgr) iter(fn func(tbl *dbMap)) {
	// it.mu.RLock()
	// defer it.mu.RUnlock()
	for _, tm := range it.arrDatabases {

		if tm.meta == nil || tm.data == nil || tm.data.ReplicaNum < 1 ||
			tm.mapMeta == nil || tm.mapData == nil {
			continue
		}

		fn(tm)
	}
}

func (it *dbMapMgr) syncDatabase(meta *kvapi.Meta, data *kvapi.Database) *dbMap {
	if meta == nil || data == nil {
		return nil
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	pt, ok := it.databases[data.Id]
	if !ok {
		pt = &dbMap{
			storeMgr:   it.storeMgr,
			cfg:        it.cfg,
			meta:       meta,
			data:       data,
			replicas:   map[uint64]*dbReplica{},
			rmReplicas: map[uint64]int64{},
		}
		it.databases[data.Id] = pt
		it.arrDatabases = append(it.arrDatabases, pt)
		it.indexes[data.Name] = data.Id
	} else if pt.meta == nil || meta.Version > pt.meta.Version {
		pt.meta = meta
		pt.data = data
	}
	return pt
}

func (it *dbMap) nextIncr() uint64 {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapData == nil {
		panic("init logic error")
	}

	for _, shard := range it.mapData.Shards {
		if it.mapData.IncrId < shard.Id {
			it.mapData.IncrId = shard.Id
		}
		for _, rep := range shard.Replicas {
			if it.mapData.IncrId < rep.Id {
				it.mapData.IncrId = rep.Id
			}
		}
	}

	if it.mapData.IncrId < 10 {
		it.mapData.IncrId = 10
	} else {
		it.mapData.IncrId += 1
	}

	return it.mapData.IncrId
}

func (it *dbMap) shardSize() int64 {
	if it.data.ShardSize >= kShardSplit_CapacitySize_Min &&
		it.data.ShardSize <= kShardSplit_CapacitySize_Max {
		return it.data.ShardSize
	}
	return kShardSplit_CapacitySize_Def
}

func (it *dbMap) getStore(id uint64) (store storage.Conn) {
	return it.storeMgr.store(id)
}

func (it *dbMap) syncMap(meta *kvapi.Meta, data *kvapi.DatabaseMap) {
	if meta == nil || data == nil {
		return
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapMeta == nil || meta.Version > it.mapMeta.Version {
		it.mapMeta = meta
		it.mapData = data
		// jsonPrint("map", data)
	}
}

func (it *dbMap) syncStore(id uint64, store storage.Conn) {
	it.storeMgr.syncStore(id, store)
}

func (it *dbMap) getShard(id uint64) (*kvapi.DatabaseMap_Shard, *kvapi.DatabaseMap_Shard) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapData != nil {
		for i, s := range it.mapData.Shards {
			if s.Id == id {
				if i+1 < len(it.mapData.Shards) {
					return s, it.mapData.Shards[i+1]
				}
				return s, nil
			}
		}
	}
	return nil, nil
}

func (it *dbMap) _hitShardConv(hit *kvapi.DatabaseMap_Shard) *dbMapSelectShard {
	slr := &dbMapSelectShard{
		mapVersion: hit.Version,
		shardId:    hit.Id,
		replicaNum: int(it.data.ReplicaNum),
		lowerKey:   bytesClone(hit.LowerKey),
	}
	it.repMut.Lock()
	defer it.repMut.Unlock()
	for _, rep := range hit.Replicas {
		if !kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
			continue
		}
		store := it.storeMgr.store(rep.StoreId)
		if store == nil {
			continue
		}
		trep, ok := it.replicas[rep.Id]
		if !ok {
			trep, err = NewDatabase(store, it.data.Id, it.data.Name, hit.Id, rep.Id, it.cfg)
			if err != nil {
				continue
			}

			it.replicas[rep.Id] = trep
			it.arrReplicas = append(it.arrReplicas, trep)
		}
		slr.replicas = append(slr.replicas, trep)
	}
	return slr
}

func (it *dbMapMgr) lookupByKey(dbName string, key []byte) *dbMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()
	id, ok := it.indexes[dbName]
	if !ok {
		return &dbMapSelectShard{}
	}
	db, ok := it.databases[id]
	if !ok {
		return &dbMapSelectShard{}
	}
	return db.lookupByKey(key)
}

func (it *dbMap) lookupByKey(key []byte) *dbMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return &dbMapSelectShard{}
	}

	var (
		hit *kvapi.DatabaseMap_Shard
	)

	if len(it.mapData.Shards) == 1 {
		hit = it.mapData.Shards[0]
	} else {
		for i, shard := range it.mapData.Shards {
			if !kvapi.AttrAllow(shard.Action, kShardSetup_In) {
				continue
			}
			if bytes.Compare(key, shard.LowerKey) < 0 {
				continue
			}
			hit = shard
			for j := i + 1; j < len(it.mapData.Shards); j++ {
				next := it.mapData.Shards[j]
				if !kvapi.AttrAllow(next.Action, kShardSetup_In) {
					continue
				}
				if bytes.Compare(key, next.LowerKey) >= 0 {
					hit = shard
				} else {
					break
				}
			}
		}
	}

	if hit != nil {
		return it._hitShardConv(hit)
	}

	return &dbMapSelectShard{}
}

func (it *dbMap) lookupByKeys(keys [][]byte) []*dbMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return nil
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	var (
		selectShards []*dbMapSelectShard
	)

	if len(it.mapData.Shards) == 1 {
		selectShards = append(selectShards, it._hitShardConv(it.mapData.Shards[0]))
		selectShards[0].keys = keys
	} else {
		for i, shard := range it.mapData.Shards {

			if !kvapi.AttrAllow(shard.Action, kShardSetup_In) {
				continue
			}

			var (
				next        *kvapi.DatabaseMap_Shard
				selectShard *dbMapSelectShard
			)

			for j := i + 1; j < len(it.mapData.Shards); j++ {
				if !kvapi.AttrAllow(it.mapData.Shards[j].Action, kShardSetup_In) {
					continue
				}
				next = it.mapData.Shards[j]
				break
			}

			for _, key := range keys {

				if bytes.Compare(key, shard.LowerKey) >= 0 {
					if next == nil || len(next.LowerKey) == 0 || bytes.Compare(key, next.LowerKey) < 0 {

						if selectShard == nil {
							selectShard = it._hitShardConv(shard)
						}

						selectShard.keys = append(selectShard.keys, key)
					}
				}
			}

			if selectShard != nil {
				selectShards = append(selectShards, selectShard)
				keys = keys[len(selectShard.keys):]
			}

			if len(keys) == 0 {
				break
			}
		}
	}

	return selectShards
}

func (it *dbMap) lookupByRange(lowerKey, upperKey []byte, rev bool) []*dbMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return nil
	}

	if bytes.Compare(lowerKey, upperKey) > 0 {
		return nil
	}

	var (
		selectShards []*dbMapSelectShard
	)

	if len(it.mapData.Shards) == 1 {
		selectShards = append(selectShards, it._hitShardConv(it.mapData.Shards[0]))
	} else {

		for i, shard := range it.mapData.Shards {

			if !kvapi.AttrAllow(shard.Action, kShardSetup_In) {
				continue
			}

			var (
				next        *kvapi.DatabaseMap_Shard
				selectShard *dbMapSelectShard
			)

			for j := i + 1; j < len(it.mapData.Shards); j++ {
				if !kvapi.AttrAllow(it.mapData.Shards[j].Action, kShardSetup_In) {
					continue
				}
				next = it.mapData.Shards[j]
				break
			}

			if len(selectShards) == 0 {

				if bytes.Compare(lowerKey, shard.LowerKey) < 0 {
					continue
				}

				selectShard = it._hitShardConv(shard)

			} else {

				if len(shard.LowerKey) == 0 || bytes.Compare(shard.LowerKey, upperKey) <= 0 {
					selectShard = it._hitShardConv(shard)
				} else {
					break
				}
			}

			if selectShard != nil {
				selectShard.lowerKey = bytesClone(shard.LowerKey)
				if next != nil {
					selectShard.upperKey = bytesClone(next.LowerKey)
				}
				selectShards = append(selectShards, selectShard)
			}
		}

		if rev && len(selectShards) > 1 {
			for i := 0; i < len(selectShards)/2; i++ {
				selectShards[i], selectShards[len(selectShards)-i-1] = selectShards[len(selectShards)-i-1], selectShards[i]
			}
		}
	}

	return selectShards
}

func dbMapShardIter(mapData *kvapi.DatabaseMap, fn func(prev, curr, next *kvapi.DatabaseMap_Shard)) {

	var shards []*kvapi.DatabaseMap_Shard

	for _, shard := range mapData.Shards {

		if !kvapi.AttrAllow(shard.Action, kShardSetup_In) {
			continue
		}

		shards = append(shards, shard)
	}

	for i, shard := range shards {
		if i == 0 {
			if i+1 < len(shards) {
				fn(nil, shard, shards[i+1])
			} else {
				fn(nil, shard, nil)
			}
		} else {
			if i+1 < len(shards) {
				fn(shards[i-1], shard, shards[i+1])
			} else {
				fn(shards[i-1], shard, nil)
			}
		}
	}
}

type dbMap_ShardStatus struct {
	replicas []*kvapi.DatabaseMapStatus_Replica
	avgSize  int64
}

func (it *dbMap) shardStatus(
	shard *kvapi.DatabaseMap_Shard,
	mapVersion uint64,
	setupAction uint64,
	statusAction uint64,
) *dbMap_ShardStatus {
	ss := &dbMap_ShardStatus{}
	for _, rep := range shard.Replicas {
		if setupAction > 0 && !kvapi.AttrAllow(rep.Action, setupAction) {
			continue
		}
		repStatus := it.statusReplica(rep.Id)
		if repStatus == nil || repStatus.MapVersion != mapVersion {
			continue
		}
		if statusAction > 0 && !kvapi.AttrAllow(repStatus.Action, statusAction) {
			continue
		}
		ss.avgSize += repStatus.Used
		ss.replicas = append(ss.replicas, repStatus)
	}
	if len(ss.replicas) > 0 {
		ss.avgSize = ss.avgSize / int64(len(ss.replicas))
	}
	return ss
}

func (it *dbMap) syncStatusReplica(repId, action uint64) *kvapi.DatabaseMapStatus_Replica {
	rep := it.statusReplica(repId)
	rep.Action = action
	rep.Updated = timesec()
	return rep
}

func (it *dbMap) setReplicaStatus(repId uint64, fn func(*kvapi.DatabaseMapStatus_Replica)) {
	it.stmut.Lock()
	defer it.stmut.Unlock()
	if it.status.Replicas == nil {
		it.status.Replicas = map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	}
	rep, ok := it.status.Replicas[repId]
	if !ok {
		rep = &kvapi.DatabaseMapStatus_Replica{}
		it.status.Replicas[repId] = rep
	}
	fn(rep)
}

func (it *dbMap) statusReplica(repId uint64) *kvapi.DatabaseMapStatus_Replica {
	it.stmut.Lock()
	defer it.stmut.Unlock()
	if it.status.Replicas == nil {
		it.status.Replicas = map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	}
	rep, ok := it.status.Replicas[repId]
	if !ok {
		rep = &kvapi.DatabaseMapStatus_Replica{
			//
		}
		it.status.Replicas[repId] = rep
	}
	return rep
}

func (it *dbMap) shardReplicaStatusList(shard *kvapi.DatabaseMap_Shard) map[uint64]*kvapi.DatabaseMapStatus_Replica {
	it.stmut.Lock()
	defer it.stmut.Unlock()
	if it.status.Replicas == nil {
		it.status.Replicas = map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	}
	reps := map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	for _, rep := range shard.Replicas {
		rs, ok := it.status.Replicas[rep.Id]
		if ok {
			reps[rep.Id] = rs
		}
	}
	return reps
}

func (it *dbMap) Close() error {
	return nil
}
