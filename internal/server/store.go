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
	"sort"
	"sync"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type storeManager struct {
	mu sync.RWMutex

	configs map[uint64]*ConfigStore
	stores  map[uint64]storage.Conn

	status storeStatusManager

	rebalancePending     int
	lastRebalanceUpdated int64

	mergePending     int
	lastMergeUpdated int64
}

type storeStatusManager struct {
	Items []*kvapi.SysStoreStatus `json:"items"`

	version uint64
	vers    map[string]uint64
	hset    map[string]*kvapi.SysStoreStatus
}

func newStoreManager() *storeManager {
	return &storeManager{
		configs: map[uint64]*ConfigStore{},
		stores:  map[uint64]storage.Conn{},
	}
}

func (it *storeManager) store(id uint64) storage.Conn {
	it.mu.Lock()
	defer it.mu.Unlock()
	if s, ok := it.stores[id]; ok {
		return s
	}
	return nil
}

func (it *storeManager) ok() int {
	it.mu.Lock()
	defer it.mu.Unlock()
	return len(it.configs)
}

func (it *storeManager) getConfig(id uint64) *ConfigStore {
	it.mu.Lock()
	defer it.mu.Unlock()
	if v, ok := it.configs[id]; ok {
		return v
	}
	return nil
}

func (it *storeManager) setConfig(cfg *ConfigStore) {
	if cfg.StoreId == 0 {
		return
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	if _, ok := it.configs[cfg.StoreId]; !ok {
		it.configs[cfg.StoreId] = cfg
	}
}

func (it *storeManager) syncStore(id uint64, store storage.Conn) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if _, ok := it.stores[id]; !ok {
		it.stores[id] = store
	}
}

func (it *storeManager) updateStatusVersion() {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.status.version += 1
}

func (it *storeManager) iterSetStatus(fn func(s *kvapi.SysStoreStatus)) {
	it.mu.Lock()
	defer it.mu.Unlock()

	for _, store := range it.status.Items {
		if store.Options == nil {
			store.Options = map[string]string{}
		}
		fn(store)
	}
}

func (it *storeManager) setStatus(uniId string, id uint64, fn func(s *kvapi.SysStoreStatus)) {
	if id == 0 {
		return
	}
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.status.hset == nil {
		it.status.hset = map[string]*kvapi.SysStoreStatus{}
		it.status.vers = map[string]uint64{}
	}

	if uniId != "" {

		if s, ok := it.status.hset[uniId]; !ok {
			s = &kvapi.SysStoreStatus{
				Id:    id,
				UniId: uniId,
			}
			it.status.hset[uniId] = s
			it.status.Items = append(it.status.Items, s)
		}
	}

	for _, store := range it.status.Items {
		if store.Id == id {
			if store.Options == nil {
				store.Options = map[string]string{}
			}
			fn(store)
			break
		}
	}
}

func (it *storeManager) lookupFitStore(deny map[uint64]bool) *kvapi.SysStoreStatus {
	it.mu.Lock()
	defer it.mu.Unlock()
	fitStores := []*kvapi.SysStoreStatus{}
	for _, vs := range it.status.Items {
		// if vs.UniId == "4cf421f1a5ca2e9a" && vs.CapacityUsed > 6e6 {
		// 	vs.CapacityUsed -= 6e6
		// 	vs.CapacityFree += 6e6
		// }
		if len(deny) == 0 || !deny[vs.Id] {
			fitStores = append(fitStores, vs)
		}
	}
	if len(fitStores) > 0 {
		sort.Slice(fitStores, func(i, j int) bool {
			return fitStores[i].CapacityFree > fitStores[j].CapacityFree
		})
		return fitStores[0]
	}
	return nil
}

func (it *storeManager) closeAll() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	for _, s := range it.stores {
		s.Close()
	}
	it.stores = map[uint64]storage.Conn{}
	return nil
}

func (it *storeManager) statusIter(fn func(s *kvapi.SysStoreStatus)) int {
	for _, s := range it.status.Items {
		fn(s)
	}
	return len(it.status.Items)
}

type storeStats struct {
	mgr *storeManager

	Items []*storeStatsItem `json:"items"`

	CapNum int

	capFree  int64
	capTotal int64

	FreeAvg      int64   `json:"free_avg"`
	FreeRatioAvg float64 `json:"free_ratio_avg"`
}

type storeStatsItem struct {
	status *kvapi.SysStoreStatus `json:"-"`

	Free      int64   `json:"free"`
	FreeRatio float64 `json:"free_ratio"`
}

func (it *storeManager) stats(name string) *storeStats {
	it.mu.Lock()
	defer it.mu.Unlock()
	st := &storeStats{
		mgr:    it,
		CapNum: len(it.status.Items),
	}
	if st.CapNum > 0 {
		if v, ok := it.status.vers[name]; ok {
			if v >= it.status.version {
				return st
			}
		}
		it.status.vers[name] = it.status.version
		for _, v := range it.status.Items {

			total := v.CapacityFree + v.CapacityUsed
			if total < 1024 { // 1 GB
				continue
			}

			item := &storeStatsItem{
				status:    v,
				Free:      v.CapacityFree,
				FreeRatio: float64(v.CapacityFree) / float64(total),
			}

			st.capFree += v.CapacityFree
			st.capTotal += total

			st.Items = append(st.Items, item)
		}
		if st.capTotal > 0 {
			st.FreeAvg = st.capFree / int64(len(it.status.Items))
			st.FreeRatioAvg = float64(st.capFree) / float64(st.capTotal)
		}
	}
	return st
}
