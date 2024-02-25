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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hooto/hlog4g/hlog"
	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
	tsd2 "github.com/valuedig/apis/go/tsd2"
)

type dbTableIncrSet struct {
	offset uint64
	cutset uint64
}

const (
	dbTableLogPullOffsetVersionCurrent uint8 = 1
)

type dbTableLogPullOffset struct {
	Version       uint8  `json:"version"`
	LogOffset     uint64 `json:"log_offset"`
	MetaKeyOffset []byte `json:"meta_key_offset"`
	MetaKeyCutset []byte `json:"meta_key_cutset"`
	DataKeyOffset []byte `json:"data_key_offset"`
	DataKeyCutset []byte `json:"data_key_cutset"`
	Address       string `json:"address"`
	TableFrom     string `json:"table_from"`
	TableTo       string `json:"table_to"`
}

type dbTable struct {
	instId         string
	tableId        uint32
	tableName      string
	db             kv2.StorageEngine
	incrMu         sync.RWMutex
	incrSets       map[string]*dbTableIncrSet
	logMu          sync.RWMutex
	logOffset      uint64
	logCutset      uint64
	logPullMu      sync.Mutex
	logPullPending map[string]bool
	logPullOffsets map[string]*dbTableLogPullOffset
	logLockSets    map[uint64]uint64
	monitor        *tsd2.SampleSet
	logSyncBuffer  *logSyncBufferTable
	closed         bool
	expiredNext    int64
	expiredMu      sync.RWMutex
}

func (tdb *dbTable) setup() error {

	tdb.logMu.Lock()
	defer tdb.logMu.Unlock()

	var (
		offset = keyEncode(nsKeyLog, []byte{0x00})
		cutset = keyEncode(nsKeyLog, []byte{0xff})
		iter   = tdb.db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})
		num = 10
	)

	for ok := iter.Last(); ok && num > 0; ok = iter.Prev() {
		num--
	}

	for ok := iter.Next(); ok; ok = iter.Next() {

		if bytes.Compare(iter.Key(), cutset) >= 0 {
			continue
		}

		if len(iter.Value()) < 2 {
			continue
		}

		meta, _, err := kv2.ObjectMetaDecode(iter.Value())
		if err == nil && meta != nil {
			tdb.logSyncBuffer.put(meta.Version, meta.Attrs, meta.Key, true)
			// hlog.Printf("info", "meta.Version %d, meta.Key %s", meta.Version, string(meta.Key))
		}
	}

	iter.Release()

	return nil
}

func (tdb *dbTable) objectLogVersionSet(incr, set, updated uint64) (uint64, error) {

	tdb.logMu.Lock()
	defer tdb.logMu.Unlock()

	if incr == 0 && set == 0 {
		return tdb.logOffset, nil
	}

	var err error

	if tdb.logCutset <= 100 {

		if ss := tdb.db.Get(keySysLogCutset, nil); !ss.OK() {
			if !ss.NotFound() {
				return 0, ss.Error()
			}
		} else {
			if tdb.logCutset, err = strconv.ParseUint(ss.String(), 10, 64); err != nil {
				return 0, err
			}
			if tdb.logOffset < tdb.logCutset {
				tdb.logOffset = tdb.logCutset
			}
		}
	}

	if tdb.logOffset < 100 {
		tdb.logOffset = 100
	}

	if set > 0 && set > tdb.logOffset {
		incr += (set - tdb.logOffset)
	}

	if incr > 0 {

		if (tdb.logOffset + incr) >= tdb.logCutset {

			cutset := tdb.logOffset + incr + 100

			if n := cutset % 100; n > 0 {
				cutset += n
			}

			if ss := tdb.db.Put(keySysLogCutset,
				[]byte(strconv.FormatUint(cutset, 10)), nil); !ss.OK() {
				return 0, ss.Error()
			}

			hlog.Printf("debug", "table %s, reset log-version to %d~%d",
				tdb.tableName, tdb.logOffset+incr, cutset)

			tdb.logCutset = cutset
		}

		tdb.logOffset += incr

		if updated > 0 {
			tdb.logLockSets[tdb.logOffset] = updated
		}
	}

	return tdb.logOffset, nil
}

func (tdb *dbTable) objectLogFree(logId uint64) {
	tdb.logMu.Lock()
	defer tdb.logMu.Unlock()
	delete(tdb.logLockSets, logId)
}

func (tdb *dbTable) objectLogDelay() uint64 {
	tdb.logMu.Lock()
	defer tdb.logMu.Unlock()
	var (
		tn    = uint64(time.Now().UnixNano() / 1e6)
		dels  = []uint64{}
		delay = tn
	)
	for k, v := range tdb.logLockSets {
		if v+3000 < tn {
			dels = append(dels, k)
		} else if v < delay {
			delay = v
		}
	}
	for _, k := range dels {
		delete(tdb.logLockSets, k)
	}
	if len(tdb.logLockSets) == 0 {
		return tn
	}
	return delay
}

func (tdb *dbTable) objectIncrSet(ns string, incr, set uint64) (uint64, error) {

	tdb.incrMu.Lock()
	defer tdb.incrMu.Unlock()

	incrSet := tdb.incrSets[ns]
	if incrSet == nil {
		incrSet = &dbTableIncrSet{
			offset: 0,
			cutset: 0,
		}
		tdb.incrSets[ns] = incrSet
	}

	if incr == 0 && set == 0 {
		return incrSet.offset, nil
	}

	var err error

	if incrSet.cutset <= 100 {

		if ss := tdb.db.Get(keySysIncrCutset(ns), nil); !ss.OK() {
			if !ss.NotFound() {
				return 0, ss.Error()
			}
		} else {
			if incrSet.cutset, err = strconv.ParseUint(ss.String(), 10, 64); err != nil {
				return 0, err
			}
			if incrSet.offset < incrSet.cutset {
				incrSet.offset = incrSet.cutset
			}
		}
	}

	if incrSet.offset < 100 {
		incrSet.offset = 100
	}

	if set > 0 && set > incrSet.offset {
		incr += (set - incrSet.offset)
	}

	if incr > 0 {

		if (incrSet.offset + incr) >= incrSet.cutset {

			cutset := incrSet.offset + incr + 100

			if ss := tdb.db.Put(keySysIncrCutset(ns),
				[]byte(strconv.FormatUint(cutset, 10)), nil); !ss.OK() {
				return 0, ss.Error()
			}

			incrSet.cutset = cutset
		}

		incrSet.offset += incr
	}

	return incrSet.offset, nil
}

func (tdb *dbTable) logPullOffsetFlush(hostAddr, tableFrom, tableTo string,
	offset *dbTableLogPullOffset, chg bool) *dbTableLogPullOffset {

	lkey := fmt.Sprintf("addr:%s/table:%s:%s", hostAddr, tableFrom, tableTo)

	tdb.logPullMu.Lock()
	defer tdb.logPullMu.Unlock()

	var (
		prevOffset, ok = tdb.logPullOffsets[lkey]
	)

	if !ok || prevOffset == nil {
		if ss := tdb.db.Get(keySysSyncLogPull(hostAddr, tableFrom, tableTo), nil); !ss.OK() {
			if !ss.NotFound() {
				return nil
			}
		} else {
			var set dbTableLogPullOffset
			if err := jsonDecode(ss.Bytes(), &set); err == nil {
				prevOffset = &set
				tdb.logPullOffsets[lkey] = prevOffset
				hlog.Printf("info", "table %s load offset %s", tdb.tableName, ss.String())
			}
		}

		if prevOffset == nil {
			prevOffset, chg = &dbTableLogPullOffset{
				Version: dbTableLogPullOffsetVersionCurrent,
			}, true
			tdb.logPullOffsets[lkey] = prevOffset
		}
	}

	if offset != nil && offset != prevOffset {
		if offset.LogOffset > prevOffset.LogOffset {
			prevOffset.LogOffset, chg = offset.LogOffset, true
		}
		if bytes.Compare(offset.DataKeyOffset, prevOffset.DataKeyOffset) > 0 {
			prevOffset.DataKeyOffset, chg = offset.DataKeyOffset, true
		}
		if bytes.Compare(offset.MetaKeyOffset, prevOffset.MetaKeyOffset) > 0 {
			prevOffset.MetaKeyOffset, chg = offset.MetaKeyOffset, true
		}
	}

	if prevOffset.Version != dbTableLogPullOffsetVersionCurrent {
		prevOffset, chg = &dbTableLogPullOffset{
			Version: dbTableLogPullOffsetVersionCurrent,
		}, true
		tdb.logPullOffsets[lkey] = prevOffset
	}

	if chg {
		if prevOffset.Address != hostAddr {
			prevOffset.Address = hostAddr
		}
		if prevOffset.TableFrom != tableFrom {
			prevOffset.TableFrom = tableFrom
		}
		if prevOffset.TableTo != tableTo {
			prevOffset.TableTo = tableTo
		}
		tdb.db.Put(keySysSyncLogPull(hostAddr, tableFrom, tableTo), jsonEncode(prevOffset), nil)
	}

	return prevOffset
}

func (it *dbTable) expiredSync(t int64) int64 {

	it.expiredMu.Lock()
	defer it.expiredMu.Unlock()

	if t == -1 {
		it.expiredNext = workerLocalExpireMax
	} else if t > 0 && t < it.expiredNext {
		it.expiredNext = t
	}

	return it.expiredNext
}

func (it *dbTable) Close() error {

	if it.closed || it.db == nil {
		return nil
	}

	for ns, incrSet := range it.incrSets {

		if incrSet.cutset > incrSet.offset {

			incrSet.cutset = incrSet.offset

			if ss := it.db.Put(keySysIncrCutset(ns),
				[]byte(strconv.FormatUint(incrSet.cutset, 10)), nil); !ss.OK() {
				hlog.Printf("info", "db error %s", ss.ErrorMessage())
			} else {
				hlog.Printf("info", "kvgo table %s, flush incr ns:%s offset %d",
					it.tableName, ns, incrSet.offset)
			}
		}
	}

	it.logMu.Lock()
	defer it.logMu.Unlock()
	if it.logCutset > it.logOffset {

		it.logCutset = it.logOffset

		if ss := it.db.Put(keySysLogCutset,
			[]byte(strconv.FormatUint(it.logCutset, 10)), nil); !ss.OK() {
			hlog.Printf("info", "db error %s", ss.ErrorMessage())
		} else {
			hlog.Printf("info", "kvgo table %s, flush log-id offset %d", it.tableName, it.logCutset)
		}
	}

	it.db.Close()

	it.closed = true

	return nil
}
