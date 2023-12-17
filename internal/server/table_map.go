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

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

type tableMapMgr struct {
	mu sync.RWMutex

	cfg *Config

	storeMgr *storeManager

	tables    map[string]*tableMap
	arrTables []*tableMap

	// table name -> id
	indexes map[string]string
}

type tableMapSelectShard struct {
	mapVersion uint64

	shardId uint64

	keys [][]byte

	lowerKey []byte
	upperKey []byte

	replicas   []*tableReplica
	replicaNum int
}

type tableMapShardStats struct {
	usageAvg int64

	lastSetUpdated int64
}

type tableMap struct {
	mu sync.RWMutex

	cfg *Config

	meta *kvapi.Meta
	data *kvapi.Table

	mapMeta *kvapi.Meta
	mapData *kvapi.TableMap

	mapIncr uint64

	storeMgr *storeManager

	repMut      sync.RWMutex
	replicas    map[uint64]*tableReplica
	arrReplicas []*tableReplica
	rmReplicas  map[uint64]int64

	stmut  sync.RWMutex
	status kvapi.TableMapStatus

	arrIndex int

	stats map[uint64]*tableMapShardStats
}

func (it *tableMap) replica(id uint64) *tableReplica {
	it.repMut.Lock()
	defer it.repMut.Unlock()
	if rep, ok := it.replicas[id]; ok {
		return rep
	}
	return nil
}

func (it *tableMap) tryRemoveReplica(repId uint64) {
	it.repMut.Lock()
	defer it.repMut.Unlock()
	// it.rmReplicas[repId] = 1
}

func (it *tableMap) tryInitReplica(shard *kvapi.TableMap_Shard, rep *kvapi.TableMap_Replica) *tableReplica {
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
		trep, err = NewTable(store, it.data.Id, it.data.Name, shard.Id, rep.Id, it.cfg)
		if err != nil {
			return nil
		}

		it.replicas[rep.Id] = trep
		it.arrReplicas = append(it.arrReplicas, trep)
	}
	return trep
}

func (it *tableMap) iterStatusDisplay(fn func(
	shard *kvapi.TableMap_Shard,
	shardStatus string,
	replicaStatus []string,
)) {
	it.mu.RLock()
	defer it.mu.RUnlock()
	for _, shard := range it.mapData.Shards {
		status := []string{}
		for _, rep := range shard.Replicas {
			if repInst := it.replica(rep.Id); repInst != nil {
				status = append(status, fmt.Sprintf("#%d %s %s %dM %ds", rep.Id,
					replicaActionDisplay(rep.Action), replicaActionDisplay(repInst.status.action),
					repInst.status.storageUsed.value/(1<<20),
					timesec()-repInst.status.storageUsed.updated,
				))
			} else {
				status = append(status, fmt.Sprintf("#%d %s %s %d -1", rep.Id,
					replicaActionDisplay(rep.Action), replicaActionDisplay(0), 0))
			}
		}
		fn(shard, shardActionDisplay(shard.Action), status)
	}
}

func newTableMapMgr(cfg *Config, storeMgr *storeManager) *tableMapMgr {
	if cfg == nil {
		panic("no config setup")
	}
	if storeMgr == nil {
		panic("no store setup")
	}
	cfg.Reset()
	mgr := &tableMapMgr{
		cfg:      cfg,
		storeMgr: storeMgr,
		tables:   map[string]*tableMap{},
		indexes:  map[string]string{},
	}
	return mgr
}

func (it *tableMapMgr) getByName(name string) *tableMap {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if id, ok := it.indexes[name]; ok {
		if t, ok := it.tables[id]; ok {
			return t
		}
	}
	return nil
}

func (it *tableMapMgr) getById(id string) *tableMap {
	it.mu.Lock()
	defer it.mu.Unlock()
	if pt, ok := it.tables[id]; ok {
		return pt
	}
	return nil
}

func (it *tableMapMgr) iter(fn func(tbl *tableMap)) {
	// it.mu.RLock()
	// defer it.mu.RUnlock()
	for _, tm := range it.arrTables {

		if tm.meta == nil || tm.data == nil || tm.data.ReplicaNum < 1 ||
			tm.mapMeta == nil || tm.mapData == nil {
			continue
		}

		fn(tm)
	}
}

func (it *tableMapMgr) syncTable(meta *kvapi.Meta, data *kvapi.Table) *tableMap {
	if meta == nil || data == nil {
		return nil
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	pt, ok := it.tables[data.Id]
	if !ok {
		pt = &tableMap{
			storeMgr:   it.storeMgr,
			cfg:        it.cfg,
			meta:       meta,
			data:       data,
			replicas:   map[uint64]*tableReplica{},
			rmReplicas: map[uint64]int64{},
			arrIndex:   len(it.tables),

			stats: map[uint64]*tableMapShardStats{},
		}
		it.tables[data.Id] = pt
		it.arrTables = append(it.arrTables, pt)
		it.indexes[data.Name] = data.Id
	} else if pt.meta == nil || meta.Version > pt.meta.Version {
		pt.meta = meta
		pt.data = data
	}
	return pt
}

func (it *tableMap) nextIncr() uint64 {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapData == nil {
		panic("init logic error")
	}

	for _, shard := range it.mapData.Shards {
		if shard.Id > it.mapData.IncrId {
			it.mapData.IncrId = shard.Id
		}
		for _, rep := range shard.Replicas {
			if rep.Id > it.mapData.IncrId {
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

func (it *tableMap) getStore(id uint64) (store storage.Conn) {
	return it.storeMgr.store(id)
}

func (it *tableMap) syncMap(meta *kvapi.Meta, data *kvapi.TableMap) {
	if meta == nil || data == nil {
		return
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapMeta == nil || meta.Version > it.mapMeta.Version {
		it.mapMeta = meta
		it.mapData = data
	}
}

func (it *tableMap) syncStore(id uint64, store storage.Conn) {
	it.storeMgr.syncStore(id, store)
}

func (it *tableMap) getShard(id uint64) (*kvapi.TableMap_Shard, *kvapi.TableMap_Shard) {
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

func (it *tableMap) _hitShardConv(hit *kvapi.TableMap_Shard) *tableMapSelectShard {
	slr := &tableMapSelectShard{
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
			trep, err = NewTable(store, it.data.Id, it.data.Name, hit.Id, rep.Id, it.cfg)
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

func (it *tableMapMgr) lookupByKey(tableName string, key []byte) *tableMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()
	id, ok := it.indexes[tableName]
	if !ok {
		return &tableMapSelectShard{}
	}
	table, ok := it.tables[id]
	if !ok {
		return &tableMapSelectShard{}
	}
	return table.lookupByKey(key)
}

func (it *tableMap) lookupByKey(key []byte) *tableMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return &tableMapSelectShard{}
	}

	var (
		hit *kvapi.TableMap_Shard
	)

	if len(it.mapData.Shards) == 1 {
		hit = it.mapData.Shards[0]
	} else {
		for i, shard := range it.mapData.Shards {
			if bytes.Compare(key, shard.LowerKey) < 0 {
				continue
			}
			if i+1 < len(it.mapData.Shards) {
				next := it.mapData.Shards[i+1]
				if len(next.LowerKey) == 0 || bytes.Compare(key, next.LowerKey) < 0 {
					hit = shard
					break
				}
			} else {
				hit = shard
				break
			}
		}
	}

	if hit != nil {
		return it._hitShardConv(hit)
	}

	return &tableMapSelectShard{}
}

func (it *tableMap) lookupByKeys(keys [][]byte) []*tableMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return nil
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	var (
		selectShards []*tableMapSelectShard
	)

	if len(it.mapData.Shards) == 1 {
		selectShards = append(selectShards, it._hitShardConv(it.mapData.Shards[0]))
		selectShards[0].keys = keys
	} else {
		for i, shard := range it.mapData.Shards {

			var (
				next        *kvapi.TableMap_Shard
				selectShard *tableMapSelectShard
			)

			if i+1 < len(it.mapData.Shards) {
				next = it.mapData.Shards[i+1]
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

func (it *tableMap) lookupByRange(lowerKey, upperKey []byte, rev bool) []*tableMapSelectShard {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.data.ReplicaNum == 0 {
		return nil
	}

	if bytes.Compare(lowerKey, upperKey) > 0 {
		return nil
	}

	var (
		selectShards []*tableMapSelectShard
	)

	if len(it.mapData.Shards) == 1 {
		selectShards = append(selectShards, it._hitShardConv(it.mapData.Shards[0]))
	} else {

		for i, shard := range it.mapData.Shards {

			var (
				next        *kvapi.TableMap_Shard
				selectShard *tableMapSelectShard
			)

			if i+1 < len(it.mapData.Shards) {
				next = it.mapData.Shards[i+1]
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

type tableMap_ShardStatus struct {
	replicas []*kvapi.TableMapStatus_Replica
	avgSize  int64
}

func (it *tableMap) shardStatus(
	shard *kvapi.TableMap_Shard,
	mapVersion uint64,
	setupAction uint64,
	statusAction uint64,
) *tableMap_ShardStatus {
	ss := &tableMap_ShardStatus{}
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

func (it *tableMap) syncStatusReplica(repId, action uint64) *kvapi.TableMapStatus_Replica {
	rep := it.statusReplica(repId)
	rep.Action = action
	rep.Updated = timesec()
	return rep
}

func (it *tableMap) statusReplica(repId uint64) *kvapi.TableMapStatus_Replica {
	it.stmut.Lock()
	defer it.stmut.Unlock()
	if it.status.Replicas == nil {
		it.status.Replicas = map[uint64]*kvapi.TableMapStatus_Replica{}
	}
	rep, ok := it.status.Replicas[repId]
	if !ok {
		rep = &kvapi.TableMapStatus_Replica{
			//
		}
		it.status.Replicas[repId] = rep
	}
	return rep
}

func (it *tableMap) Close() error {
	return nil
}
