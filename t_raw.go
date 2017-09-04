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
	"sync"

	"github.com/lessos/lessgo/types"
	"github.com/lynkdb/iomix/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	t_raw_incr_mu sync.Mutex
)

func (cn *Conn) RawNew(key, value []byte, ttl int64) *skv.Result {

	if data, err := cn.db.Get(key, nil); err == nil && len(data) > 0 {
		return skv.NewResult(0)
	}

	return cn.RawPut(key, value, ttl)
}

func (cn *Conn) RawDel(keys ...[]byte) *skv.Result {

	batch := new(leveldb.Batch)

	for _, key := range keys {
		batch.Delete(key)
	}

	if err := cn.db.Write(batch, nil); err != nil {
		return skv.NewResultError(skv.ResultServerError, err.Error())
	}

	return skv.NewResult(0)
}

func (cn *Conn) RawGet(key []byte) *skv.Result {

	data, err := cn.db.Get(key, nil)

	if err != nil {

		if err.Error() == "leveldb: not found" {
			return skv.NewResultNotFound()
		}

		return skv.NewResultError(skv.ResultServerError, err.Error())
	}

	return &skv.Result{
		Status: skv.ResultOK,
		Data:   [][]byte{data},
	}
}

func (cn *Conn) RawPut(key, value []byte, ttl int64) *skv.Result {

	if len(key) < 2 {
		return skv.NewResultBadArgument()
	}

	if ttl > 0 {

		if ttl < 1000 {
			return skv.NewResultBadArgument()
		}

		if ok := cn.raw_ssttlat_put(key, uint64(types.MetaTimeNow().AddMillisecond(ttl))); !ok {
			return skv.NewResultBadArgument()
		}
	}

	if err := cn.db.Put(key, value, nil); err != nil {
		return skv.NewResultError(skv.ResultServerError, err.Error())
	}

	return skv.NewResult(0)
}

func (cn *Conn) RawScan(offset, cutset []byte, limit int) *skv.Result {

	if len(cutset) < 1 {
		cutset = offset
	}

	for i := len(cutset); i < 200; i++ {
		cutset = append(cutset, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	} else if limit < 1 {
		limit = 1
	}

	var (
		rs   = skv.NewResult(0)
		iter = cn.db.NewIterator(&util.Range{
			Start: offset,
			Limit: cutset,
		}, nil)
	)

	for iter.Next() {

		if limit < 1 {
			break
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func (cn *Conn) RawRevScan(offset, cutset []byte, limit int) *skv.Result {

	if len(offset) < 1 {
		offset = cutset
	}

	for i := len(offset); i < 256; i++ {
		offset = append(offset, 0x00)
	}

	for i := len(cutset); i < 256; i++ {
		cutset = append(cutset, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	} else if limit < 1 {
		limit = 1
	}

	var (
		rs   = skv.NewResult(0)
		iter = cn.db.NewIterator(&util.Range{Start: offset, Limit: cutset}, nil)
	)

	for ok := iter.Last(); ok; ok = iter.Prev() {

		if limit < 1 {
			break
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func (cn *Conn) raw_ssttlat_put(key []byte, ttlat uint64) bool {

	if ttlat < 20060102150405000 {
		return true
	}

	//
	meta := skv.ValueMeta{}

	if rs := cn.RawGet(t_ns_cat(ns_meta, key)); rs.OK() {

		if err := rs.Decode(&meta); err != nil {
			return false
		}

	} else if !rs.NotFound() {
		return false
	}

	if ttlat == meta.Expired {
		return true
	}

	batch := new(leveldb.Batch)

	if meta.Expired > 0 {
		batch.Delete(t_ns_cat(ns_ttl, append(uint64_to_bytes(meta.Expired), key...)))
	}

	meta.Expired = ttlat

	if value_enc, err := skv.ValueEncode(&meta, nil); err == nil {

		batch.Put(t_ns_cat(ns_meta, key), value_enc)
		batch.Put(t_ns_cat(ns_ttl, append(uint64_to_bytes(ttlat), key...)), []byte{0x00})

		if err := cn.db.Write(batch, nil); err != nil {
			return false
		}
	}

	return true
}
