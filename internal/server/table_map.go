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
	"sort"
	"sync"

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

type tableMapMgr struct {
	mu sync.RWMutex

	cfg *Config

	tables    map[string]*tableMap
	arrTables []*tableMap

	// table name -> id
	indexes map[string]string
}

type tableMapSelectShard struct {
	shardId uint64

	keys [][]byte

	lowerKey []byte
	upperKey []byte

	replicas   []*tableReplica
	replicaNum int
}

type tableMap struct {
	mu sync.RWMutex

	cfg *Config

	meta *kvapi.Meta
	data *kvapi.Table

	mapMeta *kvapi.Meta
	mapData *kvapi.TableMap

	stores map[string]storage.Conn

	replicas map[uint64]*tableReplica

	arrIndex int
}

func newTableMapMgr(cfg *Config) *tableMapMgr {
	if cfg == nil {
		panic("no config setup")
	}
	cfg.Reset()
	mgr := &tableMapMgr{
		cfg:     cfg,
		tables:  map[string]*tableMap{},
		indexes: map[string]string{},
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
	for _, tbl := range it.arrTables {
		fn(tbl)
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
			cfg:      it.cfg,
			meta:     meta,
			data:     data,
			stores:   map[string]storage.Conn{},
			replicas: map[uint64]*tableReplica{},
			arrIndex: len(it.tables),
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

func (it *tableMap) getStore(id string) (store storage.Conn) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if s, ok := it.stores[id]; ok {
		return s
	}
	return nil
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

func (it *tableMap) syncStore(id string, store storage.Conn) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if _, ok := it.stores[id]; !ok {
		it.stores[id] = store
	}
}

func (it *tableMap) getShard(id uint64) *kvapi.TableMap_Shard {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.mapData != nil {
		for _, s := range it.mapData.Shards {
			if s.Id == id {
				return s
			}
		}
	}
	return nil
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
		slr = &tableMapSelectShard{}
		err error
	)
	for _, shard := range it.mapData.Shards {
		if bytes.Compare(shard.LowerKey, key) <= 0 &&
			(len(shard.UpperKey) == 0 || bytes.Compare(key, shard.UpperKey) <= 0) {
			slr.shardId = shard.Id
			slr.replicaNum = int(it.data.ReplicaNum)
			for _, rep := range shard.Replicas {
				store, ok := it.stores[rep.StoreId]
				if !ok {
					continue
				}
				trep, ok := it.replicas[rep.Id]
				if !ok {
					trep, err = NewTable(store, it.data.Id, it.data.Name, shard.Id, rep.Id, it.cfg)
					if err != nil {
						continue
					}
					it.replicas[rep.Id] = trep
				}
				slr.replicas = append(slr.replicas, trep)
			}
			break
		}
	}

	return slr
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
		err          error
	)
	for _, shard := range it.mapData.Shards {

		var selectShard *tableMapSelectShard

		for _, key := range keys {

			if bytes.Compare(shard.LowerKey, key) <= 0 &&
				(len(shard.UpperKey) == 0 || bytes.Compare(key, shard.UpperKey) <= 0) {

				if selectShard == nil {
					selectShard = &tableMapSelectShard{
						shardId:    shard.Id,
						replicaNum: int(it.data.ReplicaNum),
					}
				}

				selectShard.keys = append(selectShard.keys, key)

				for _, rep := range shard.Replicas {
					store, ok := it.stores[rep.StoreId]
					if !ok {
						continue
					}
					trep, ok := it.replicas[rep.Id]
					if !ok {
						trep, err = NewTable(store, it.data.Id, it.data.Name, shard.Id, rep.Id, it.cfg)
						if err != nil {
							continue
						}
						it.replicas[rep.Id] = trep
					}
					selectShard.replicas = append(selectShard.replicas, trep)
				}
			}
		}

		if selectShard == nil {
			continue
		}

		selectShards = append(selectShards, selectShard)
		keys = keys[len(selectShard.keys):]

		if len(keys) == 0 {
			break
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
		err          error
	)

	for _, shard := range it.mapData.Shards {

		var selectShard *tableMapSelectShard

		if len(selectShards) == 0 {

			if bytes.Compare(shard.LowerKey, lowerKey) <= 0 &&
				(len(shard.UpperKey) == 0 || bytes.Compare(lowerKey, shard.UpperKey) <= 0) {

				selectShard = &tableMapSelectShard{
					shardId:    shard.Id,
					replicaNum: int(it.data.ReplicaNum),
				}
			}

		} else {

			if bytes.Compare(shard.LowerKey, upperKey) <= 0 &&
				(len(shard.UpperKey) == 0 || bytes.Compare(upperKey, shard.UpperKey) <= 0) {

				selectShard = &tableMapSelectShard{
					shardId:    shard.Id,
					replicaNum: int(it.data.ReplicaNum),
				}
			}
		}

		if selectShard != nil {
			for _, rep := range shard.Replicas {
				store, ok := it.stores[rep.StoreId]
				if !ok {
					continue
				}
				trep, ok := it.replicas[rep.Id]
				if !ok {
					trep, err = NewTable(store, it.data.Id, it.data.Name, shard.Id, rep.Id, it.cfg)
					if err != nil {
						continue
					}
					it.replicas[rep.Id] = trep
				}
				selectShard.replicas = append(selectShard.replicas, trep)
			}
			selectShard.lowerKey = bytesClone(shard.LowerKey)
			selectShard.upperKey = bytesClone(shard.UpperKey)
			selectShards = append(selectShards, selectShard)
		}
	}

	if rev && len(selectShards) > 1 {
		for i := 0; i < len(selectShards)/2; i++ {
			selectShards[i], selectShards[len(selectShards)-i-1] = selectShards[len(selectShards)-i-1], selectShards[i]
		}
	}

	return selectShards
}

func (it *tableMap) Close() error {
	return nil
}
