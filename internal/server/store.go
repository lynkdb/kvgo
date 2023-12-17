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

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

type storeManager struct {
	mu sync.RWMutex

	configs map[uint64]*ConfigStore
	stores  map[uint64]storage.Conn

	status storeStatusManager
}

type storeStatusManager struct {
	version uint64
	vers    map[string]uint64
	items   map[string]*kvapi.SysStoreStatus
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

func (it *storeManager) syncStatus(uniId string, id uint64, used, free int64) {
	if id == 0 {
		return
	}
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.status.items == nil {
		it.status.items = map[string]*kvapi.SysStoreStatus{}
		it.status.vers = map[string]uint64{}
	}

	s, ok := it.status.items[uniId]
	if !ok {
		s = &kvapi.SysStoreStatus{
			Id:    id,
			UniId: uniId,
		}
		it.status.items[uniId] = s
	}
	s.CapacityUsed = uint64(used)
	s.CapacityFree = uint64(free)
	s.Updated = uint64(timesec())
}

func (it *storeManager) lookupFitStore(deny map[uint64]bool) *kvapi.SysStoreStatus {
	it.mu.Lock()
	defer it.mu.Unlock()
	fitStores := []*kvapi.SysStoreStatus{}
	for _, vs := range it.status.items {
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
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.status.items != nil {
		for _, s := range it.status.items {
			fn(s)
		}
	}
	return len(it.status.items)
}

type storeStats struct {
	Items []*storeStatsItem `json:"items"`

	CapNum   int
	capUsed  float64
	capTotal float64

	UsedRatioAvg float64 `json:"used_ratio_avg"`
}

type storeStatsItem struct {
	status    *kvapi.SysStoreStatus
	UsedRatio float64 `json:"used_ratio"`
	CapFree   int64
}

func (it *storeManager) stats(name string) *storeStats {
	it.mu.Lock()
	defer it.mu.Unlock()
	st := &storeStats{
		CapNum: len(it.status.items),
	}
	if st.CapNum > 0 {
		if v, ok := it.status.vers[name]; ok {
			if v >= it.status.version {
				return st
			}
		}
		it.status.vers[name] = it.status.version
		for _, v := range it.status.items {
			cap := float64(v.CapacityFree + v.CapacityUsed)
			if cap < 1024 { // 1 GB
				continue
			}

			item := &storeStatsItem{
				status:    v,
				UsedRatio: float64(v.CapacityUsed) / cap,
				CapFree:   int64(v.CapacityFree),
			}

			st.capUsed += float64(v.CapacityUsed)
			st.capTotal += cap

			st.Items = append(st.Items, item)
		}
		if st.capTotal > 0 {
			st.UsedRatioAvg = st.capUsed / st.capTotal
		}
	}
	return st
}

func (it *storeStats) sort() {
	sort.Slice(it.Items, func(i, j int) bool {
		return it.Items[i].UsedRatio < it.Items[j].UsedRatio
	})
}
