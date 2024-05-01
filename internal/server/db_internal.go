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
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type dbIncrDatabase struct {
	mu    sync.RWMutex                                   `json:"-"`
	def   *kvapi.DatabaseMapStatus_IncrOffset            `json:"-"`
	idx   map[string]*kvapi.DatabaseMapStatus_IncrOffset `json:"-"`
	name  string                                         `json:"-"`
	store storage.Conn                                   `json:"-"`
	chg   bool                                           `json:"-"`
	cache map[uint64][]int64                             `json:"-"`

	Id     string                                `json:"id"`
	Items  []*kvapi.DatabaseMapStatus_IncrOffset `json:"items,omitempty"`
	NextId uint32                                `json:"next_id,omitempty"`
}

type dbIncrManager struct {
	mu    sync.RWMutex
	store storage.Conn
	dbs   map[string]*dbIncrDatabase
}

func newIncrManager(store storage.Conn) (*dbIncrManager, error) {

	var (
		mgr = &dbIncrManager{
			store: store,
			dbs:   map[string]*dbIncrDatabase{},
		}
		iter, _ = store.NewIterator(&storage.IterOptions{
			LowerKey: nsSysDatabaseIncr(""),
			UpperKey: nsSysDatabaseIncr("zzzz"),
		})
	)
	defer iter.Release()

	testPrintf("load incr ... ...")

	for ok := iter.SeekToFirst(); ok; ok = iter.Next() {

		var dbIncr dbIncrDatabase
		if err := jsonDecode(iter.Value(), &dbIncr); err != nil {
			return nil, err
		}

		for _, item := range dbIncr.Items {
			if item.Offset < item.Cutset {
				item.Offset = item.Cutset
			}
		}

		dbIncr.init(store)

		mgr.dbs[dbIncr.Id] = &dbIncr
		hlog.Printf("info", "init load incr database %s", string(jsonEncode(dbIncr)))
	}

	return mgr, nil
}

func (it *dbIncrManager) db(id string) (*dbIncrDatabase, error) {
	if id == "" {
		panic("invalid id")
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	item, ok := it.dbs[id]
	if !ok {
		if obj, err := newIncrDatabase(id, it.store); err != nil {
			return nil, err
		} else {
			item = obj
			it.dbs[id] = item
		}
	}

	return item, nil
}

func (it *dbIncrManager) flush() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	for _, db := range it.dbs {
		db.flush()
	}

	return nil
}

func newIncrDatabase(id string, store storage.Conn) (*dbIncrDatabase, error) {

	var obj dbIncrDatabase
	if rs := store.Get(nsSysDatabaseIncr(id), nil); rs.NotFound() {
		//
	} else if !rs.OK() {
		return nil, rs.Error()
	} else if err := jsonDecode(rs.Bytes(), &obj); err != nil {
		return nil, err
	}

	for _, item := range obj.Items {
		if item.Offset < item.Cutset {
			item.Offset = item.Cutset
		}
	}

	obj.Id = id
	obj.init(store)

	return &obj, nil
}

func (it *dbIncrDatabase) init(store storage.Conn) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.idx == nil {
		it.idx = map[string]*kvapi.DatabaseMapStatus_IncrOffset{}
	}
	if it.cache == nil {
		it.cache = map[uint64][]int64{}
	}
	for _, item := range it.Items {
		it.idx[item.Ns] = item
	}
	it.store = store
}

func (it *dbIncrDatabase) sync(ns string, incr, set uint64, key []byte) (uint32, uint64, error) {

	const preAlloc uint64 = 100

	if ns == "" {
		ns = kSysIncr_NsDef
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.def == nil {
		if it.def, _ = it.idx[kSysIncr_NsDef]; it.def == nil {
			it.def = &kvapi.DatabaseMapStatus_IncrOffset{
				Ns:     kSysIncr_NsDef,
				Id:     0,
				Offset: 100,
			}
			it.idx[kSysIncr_NsDef] = it.def
			it.Items = append(it.Items, it.def)
			it.chg = true
		}
		testPrintf("incr init def, db %s, ns %s, offset %s", it.Id, ns, string(jsonEncode(it.def)))
	}

	item, ok := it.idx[ns]

	if !ok {

		it.chg = true
		it.NextId += 1

		item = &kvapi.DatabaseMapStatus_IncrOffset{
			Ns:     ns,
			Id:     it.NextId,
			Offset: it.def.Offset,
		}

		it.idx[ns] = item
		it.Items = append(it.Items, item)

		testPrintf("incr item init, db %s, item %s", it.Id, string(jsonEncode(item)))
	}

	if item.Offset < 100 {
		item.Offset = 100
	}

	if incr == 0 && set == 0 {
		return item.Id, item.Offset, nil
	}

	var kid uint64 = 0
	if len(key) > 0 {
		kid = xxhash.Sum64(key)
		if p, ok := it.cache[kid]; ok {
			return item.Id, uint64(p[0]), nil
		}
	}

	if set > 0 && set > item.Offset {
		incr += (set - item.Offset)
		// testPrintf("incr item, db %s, ns %s, set -> incr %d %d, offset %d, cutset %d",
		// 	it.Id, ns, set, incr, item.Offset, item.Cutset)
	}

	if incr > 0 {
		item.Offset += incr
		if item.Offset >= item.Cutset {
			item.Cutset = item.Offset + preAlloc
			if x := item.Cutset % preAlloc; x > 0 {
				item.Cutset -= x
			}
			it.chg = true
		}
	}

	// testPrintf("incr item, db %s, ns %s, set -> incr %d %d, offset %d, cutset %d",
	// 	it.Id, ns, set, incr, item.Offset, item.Cutset)

	if it.chg {
		// testPrintf("incr item, db %s, flush %s", it.Id, string(jsonEncode(it)))
		if rs := it.store.Put(nsSysDatabaseIncr(it.Id), jsonEncode(it), &storage.WriteOptions{
			Sync: true,
		}); !rs.OK() {
			return 0, 0, rs.Error()
		}
		it.chg = false
		hlog.Printf("info", "db #%s incr flush %s", it.Id, string(jsonEncode(it)))
	}

	// testPrintf("incr item, db %s, ns %s, set %d, offset %d, cutset %d", it.Id, ns, set, item.Offset, item.Cutset)

	if len(it.cache) > 1000 {
		tn := time.Now().Unix()
		dels := []uint64{}
		for k, v := range it.cache {
			if v[1]+10 < tn {
				dels = append(dels, k)
			}
		}
		for _, k := range dels {
			delete(it.cache, k)
		}
	}
	if len(key) > 0 {
		it.cache[kid] = []int64{int64(item.Offset), time.Now().Unix()}
	}
	return item.Id, item.Offset, nil
}

func (it *dbIncrDatabase) flush() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	for _, item := range it.Items {
		if item.Cutset > item.Offset {
			item.Cutset = item.Offset
		}
	}

	if rs := it.store.Put(nsSysDatabaseIncr(it.Id), jsonEncode(it), &storage.WriteOptions{
		Sync: true,
	}); !rs.OK() {
		hlog.Printf("info", "db error %s", rs.ErrorMessage())
	} else {
		hlog.Printf("info", "kvgo database %s, flush offset %s", it.Id, jsonEncode(it))
	}

	return nil
}
