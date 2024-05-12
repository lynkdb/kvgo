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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type dbReplica struct {
	mu sync.RWMutex

	inited bool

	dbId   string
	dbName string

	shardId   uint64
	replicaId uint64

	cfg *Config

	store storage.Conn

	incrMgr *dbIncrDatabase

	verMu     sync.RWMutex
	verOffset uint64
	verCutset uint64

	logMu    sync.RWMutex
	logState dbReplicaLogState

	taskMut sync.Map

	expiredNext  int64
	expiredMu    sync.RWMutex
	ttlRefreshed int64

	proposals  map[string]*proposalx
	proposalMu sync.RWMutex

	localStatus dbReplicaStatus

	close bool
}

type dbReplicaStatusItem struct {
	mapVersion uint64
	value      int64
	attr       uint64
	updated    int64
}

type dbReplicaStatus struct {
	mu sync.Mutex

	kvWriteKeys atomic.Int64
	kvWriteSize atomic.Int64

	action      dbReplicaStatusItem
	storageUsed dbReplicaStatusItem

	pulls     map[uint64]*dbReplicaStatusItem
	iterPulls []*dbReplicaStatusItem
}

type proposalx struct {
	id      uint64
	write   *kvapi.WriteProposalRequest
	delete  *kvapi.DeleteProposalRequest
	expired int64
}

var (
	dbReplicaMut sync.Mutex
	dbReplicaSet = map[string]*dbReplica{}
)

func (it *dbReplicaStatus) pull(id uint64) *dbReplicaStatusItem {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.pulls == nil {
		it.pulls = map[uint64]*dbReplicaStatusItem{}
	}
	item, ok := it.pulls[id]
	if !ok {
		item = &dbReplicaStatusItem{}
		it.pulls[id] = item
		it.iterPulls = append(it.iterPulls, item)
	}
	return item
}

func NewDatabase(
	store storage.Conn,
	dbId, dbName string,
	shardId, replicaId uint64,
	cfg *Config,
	args ...interface{},
) (*dbReplica, error) {

	if cfg == nil {
		panic("config not setup")
	}

	dbReplicaMut.Lock()
	defer dbReplicaMut.Unlock()

	k := fmt.Sprintf("%s.%d", dbId, replicaId)
	if dbId == sysDatabaseId {
		k = cfg.Storage.DataDirectory
	}

	dt, ok := dbReplicaSet[k]
	if ok {
		return dt, nil
	}

	dt = &dbReplica{
		store:     store,
		dbName:    dbName,
		shardId:   shardId,
		replicaId: replicaId,
		proposals: map[string]*proposalx{},
		cfg:       cfg,
	}

	for _, arg := range args {
		if arg == nil {
			continue
		}
		switch arg.(type) {
		case *dbIncrDatabase:
			dt.incrMgr = arg.(*dbIncrDatabase)
		}
	}

	if dt.incrMgr == nil {
		dt.incrMgr, err = newIncrDatabase(dbId, store)
		if err != nil {
			return nil, err
		}
	}

	dt.cfg.Reset()

	if rs := dt.store.Get(keySysInstanceId, nil); rs.OK() {
		dt.dbId = string(rs.Bytes())
		if dt.dbId != dbId {
			return nil, fmt.Errorf("database id conflict, db %s, mem %s, store info %v", dt.dbId, dbId, *dt.store.Info())
		}
	} else if rs.NotFound() {
		dt.dbId = dbId
		rs = dt.store.Put(keySysInstanceId, []byte(dt.dbId), &storage.WriteOptions{
			Sync: true,
		})
	} else if !rs.OK() {
		return nil, rs.Error()
	}

	if err := dt.init(); err != nil {
		return nil, err
	}

	dbReplicaSet[k] = dt

	return dt, nil
}

func (it *dbReplica) init() error {

	var err error

	// load log state
	if rs := it.store.Get(keySysLogState, nil); rs.NotFound() {
		//
	} else if !rs.OK() {
		return rs.Error()
	} else if err := jsonDecode(rs.Bytes(), &it.logState); err != nil {
		return err
	} else if it.logState.Offset < it.logState.Cutset {
		it.logState.Offset = it.logState.Cutset
	}
	hlog.Printf("info", "database %s, load log-state %s",
		it.dbName, string(jsonEncode(it.logState)))

	// load version state
	if rs := it.store.Get(keySysVerCutset, nil); rs.NotFound() {

	} else if !rs.OK() {
		return rs.Error()
	} else {
		if it.verCutset, err = strconv.ParseUint(string(rs.Bytes()), 10, 64); err != nil {
			return err
		} else if it.verOffset < it.verCutset {
			it.verOffset = it.verCutset
		}
	}

	return nil
}

func (it *dbReplica) versionSync(incr, set uint64) (uint64, error) {

	it.verMu.Lock()
	defer it.verMu.Unlock()

	if incr == 0 && set == 0 {
		return it.verOffset, nil
	}

	var err error

	if it.verCutset <= 100 {

		if rs := it.store.Get(keySysVerCutset, nil); !rs.OK() {
			if !rs.NotFound() {
				return 0, rs.Error()
			}
		} else {
			if it.verCutset, err = strconv.ParseUint(string(rs.Bytes()), 10, 64); err != nil {
				return 0, err
			} else if it.verOffset < it.verCutset {
				it.verOffset = it.verCutset
			}
		}
	}

	if it.verOffset < 100 {
		it.verOffset = 100
	}

	if set > 0 && set > it.verOffset {
		incr += (set - it.verOffset)
	}

	if incr > 0 {

		if (it.verOffset + incr) >= it.verCutset {

			cutset := it.verOffset + incr + 100

			if n := cutset % 100; n > 0 {
				cutset += n
			}

			if ss := it.store.Put(keySysVerCutset,
				[]byte(strconv.FormatUint(cutset, 10)), &storage.WriteOptions{
					Sync: true,
				}); !ss.OK() {
				return 0, ss.Error()
			}

			hlog.Printf("debug", "database %s, reset version to %d~%d",
				it.dbName, it.verOffset+incr, cutset)

			it.verCutset = cutset
		}

		it.verOffset += incr
	}

	return it.verOffset, nil
}

func (it *dbReplica) logSync(incr, reten uint64) (uint64, error) {

	// it.logMu.Lock()
	// defer it.logMu.Unlock()

	const logRange uint64 = 10000

	if it.logState.Offset <= logRange {

		if rs := it.store.Get(keySysLogState, nil); rs.NotFound() {
			//
		} else if !rs.OK() {
			return 0, rs.Error()
		} else if err := jsonDecode(rs.Bytes(), &it.logState); err != nil {
			return 0, err
		} else if it.logState.Offset < it.logState.Cutset {
			it.logState.Offset = it.logState.Cutset
		}
	}

	if it.logState.Offset < logRange {
		it.logState.Offset = logRange
	}

	if incr == 0 && reten == 0 {
		return it.logState.Offset, nil
	}

	if (it.logState.Offset+incr) >= it.logState.Cutset ||
		reten > it.logState.RetentionOffset {

		if reten > it.logState.RetentionOffset {
			it.logState.RetentionOffset = reten
		}

		logStateNew := it.logState
		if (it.logState.Offset + incr) >= it.logState.Cutset {
			logStateNew.Cutset += incr + logRange
		}

		if ss := it.store.Put(keySysLogState, jsonEncode(&logStateNew), &storage.WriteOptions{
			Sync: true,
		}); !ss.OK() {
			return 0, ss.Error()
		}
		it.logState.Cutset = logStateNew.Cutset

		hlog.Printf("debug", "database %s, reset log-id to %d~%d, retention %d",
			it.dbName, it.logState.Offset+incr, it.logState.Cutset, it.logState.RetentionOffset)
	}

	it.logState.Offset += incr

	return it.logState.Offset, nil
}

func (it *dbReplica) Close() error {

	if it.close || it.store == nil {
		return nil
	}
	it.close = true

	it.incrMgr.flush()

	{
		it.verMu.Lock()
		defer it.verMu.Unlock()

		if it.verCutset > it.verOffset {

			it.verCutset = it.verOffset

			if rs := it.store.Put(keySysVerCutset,
				[]byte(strconv.FormatUint(it.verCutset, 10)), &storage.WriteOptions{
					Sync: true,
				}); !rs.OK() {
				hlog.Printf("info", "db error %s", rs.ErrorMessage())
			} else {
				hlog.Printf("info", "kvgo database %s, flush log-id offset %d", it.dbName, it.verCutset)
			}
		}
	}

	it.store.Close()

	return nil
}
