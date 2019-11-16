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
	"time"

	"github.com/lynkdb/iomix/sko"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	skoKeySysLogCutset = append([]byte{ns_sko_sys}, []byte("log:cutset")...)
)

func skoKeySysLogAsync(hostport string) []byte {
	return append([]byte{ns_sko_sys},
		[]byte(fmt.Sprintf("log:async:%s", hostport))...)
}

func skoKeySysIncrCutset(ns string) []byte {
	if ns == "" {
		ns = "common"
	}
	return append([]byte{ns_sko_sys}, []byte("incr:cutset:"+ns)...)
}

func (cn *SkoConn) Commit(rr *sko.ObjectWriter) *sko.ObjectResult {

	if cn.skoCluster != nil {
		rs, err := cn.skoCluster.Commit(nil, rr)
		if err != nil {
			return sko.NewObjectResultServerError(err)
		}
		return rs
	}

	return cn.objectCommitLocal(rr, 0)
}

func (cn *SkoConn) objectCommitLocal(rr *sko.ObjectWriter, cLog uint64) *sko.ObjectResult {

	if err := rr.CommitValid(); err != nil {
		return sko.NewObjectResultClientError(err)
	}

	cn.skoMu.Lock()
	defer cn.skoMu.Unlock()

	meta, err := cn.objectMetaGet(rr)
	if meta == nil && err != nil {
		return sko.NewObjectResultServerError(err)
	}

	if meta == nil {

		if sko.AttrAllow(rr.Mode, sko.ObjectWriterModeDelete) {
			return sko.NewObjectResultOK()
		}

	} else {

		if (cLog > 0 && meta.Version == cLog) ||
			sko.AttrAllow(rr.Mode, sko.ObjectWriterModeCreate) ||
			(rr.Meta.Expired == meta.Expired && meta.DataCheck == rr.Meta.DataCheck) {

			rs := sko.NewObjectResultOK()
			rs.Meta = &sko.ObjectMeta{
				Version: meta.Version,
				IncrId:  meta.IncrId,
				Created: meta.Created,
			}

			return rs
		}

		if meta.IncrId > 0 {
			rr.Meta.IncrId = meta.IncrId
		}

		if meta.Created > 0 {
			rr.Meta.Created = meta.Created
		}
	}

	if rr.Meta.Created < 1 {
		rr.Meta.Created = rr.Meta.Updated
	}

	if rr.IncrNamespace != "" {

		if rr.Meta.IncrId == 0 {
			rr.Meta.IncrId, err = cn.objectIncrSet(rr.IncrNamespace, 1, 0)
			if err != nil {
				return sko.NewObjectResultServerError(err)
			}
		} else {
			cn.objectIncrSet(rr.IncrNamespace, 0, rr.Meta.IncrId)
		}
	}

	if cLog == 0 {
		if meta != nil && meta.Version > 0 {
			cLog = meta.Version
		}

		cLog, err = cn.objectLogVersionSet(1, cLog)
		if err != nil {
			return sko.NewObjectResultServerError(err)
		}
	}
	rr.Meta.Version = cLog

	if sko.AttrAllow(rr.Mode, sko.ObjectWriterModeDelete) {

		rr.Meta.Attrs = sko.ObjectMetaAttrDelete

		if bsMeta, err := rr.MetaEncode(); err == nil {

			batch := new(leveldb.Batch)

			if meta != nil {
				batch.Delete(t_ns_cat(ns_sko_meta, rr.Meta.Key))
				batch.Delete(t_ns_cat(ns_sko_data, rr.Meta.Key))
				batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
			}

			batch.Put(t_ns_cat(ns_sko_log, uint64_to_bytes(cLog)), bsMeta)

			err = cn.db.Write(batch, nil)
		}

	} else {

		if bsMeta, bsData, err := rr.PutEncode(); err == nil {

			batch := new(leveldb.Batch)

			batch.Put(t_ns_cat(ns_sko_meta, rr.Meta.Key), bsMeta)
			batch.Put(t_ns_cat(ns_sko_data, rr.Meta.Key), bsData)
			batch.Put(t_ns_cat(ns_sko_log, uint64_to_bytes(cLog)), bsMeta)

			if rr.Meta.Expired > 0 {
				batch.Put(keyExpireEncode(ns_sko_ttl, rr.Meta.Expired, rr.Meta.Key), bsMeta)
			}

			if meta != nil {
				if meta.Version < cLog {
					batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
				}
				if meta.Expired > 0 && meta.Expired != rr.Meta.Expired {
					batch.Delete(keyExpireEncode(ns_sko_ttl, meta.Expired, rr.Meta.Key))
				}
			}

			err = cn.db.Write(batch, nil)
		}
	}

	if err != nil {
		return sko.NewObjectResultServerError(err)
	}

	rs := sko.NewObjectResultOK()
	rs.Meta = &sko.ObjectMeta{
		Version: cLog,
		IncrId:  rr.Meta.IncrId,
	}

	return rs
}

func (cn *SkoConn) Query(rr *sko.ObjectReader) *sko.ObjectResult {

	rs := sko.NewObjectResultOK()

	if sko.AttrAllow(rr.Mode, sko.ObjectReaderModeKey) {

		for _, k := range rr.Keys {

			bs, err := cn.db.Get(t_ns_cat(ns_sko_data, k), nil)
			if err == nil {

				item, err := sko.ObjectItemDecode(bs)
				if err == nil {
					rs.Items = append(rs.Items, item)
				} else {
					rs.StatusMessage(sko.ResultServerError, err.Error())
				}

			} else {

				if err.Error() != ldbNotFound {
					rs.StatusMessage(sko.ResultServerError, err.Error())
					break
				}

				if len(rr.Keys) == 1 {
					rs.StatusMessage(sko.ResultNotFound, "")
				}
			}
		}

	} else if sko.AttrAllow(rr.Mode, sko.ObjectReaderModeKeyRange) {

		if err := cn.objectQueryKeyRange(rr, rs); err != nil {
			rs.StatusMessage(sko.ResultServerError, err.Error())
		}

	} else if sko.AttrAllow(rr.Mode, sko.ObjectReaderModeLogRange) {

		if err := cn.objectQueryLogRange(rr, rs); err != nil {
			rs.StatusMessage(sko.ResultServerError, err.Error())
		}

	} else {

		rs.StatusMessage(sko.ResultClientError, "invalid mode")
	}

	if rs.Status == 0 {
		rs.Status = sko.ResultOK
	}

	return rs
}

func (cn *SkoConn) objectQueryKeyRange(rr *sko.ObjectReader, rs *sko.ObjectResult) error {

	var (
		offset    = t_ns_cat(ns_sko_data, bytes_clone(rr.KeyOffset))
		cutset    = t_ns_cat(ns_sko_data, bytes_clone(rr.KeyCutset))
		limitNum  = rr.LimitNum
		limitSize = rr.LimitSize
	)

	if limitNum > sko.ObjectReaderLimitNumMax {
		limitNum = sko.ObjectReaderLimitNumMax
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = sko.ObjectReaderLimitSizeDef
	} else if limitSize > sko.ObjectReaderLimitSizeMax {
		limitSize = sko.ObjectReaderLimitSizeMax
	}

	var (
		iter   iterator.Iterator
		values = [][]byte{}
	)

	if sko.AttrAllow(rr.Mode, sko.ObjectReaderModeRevRange) {

		// offset = append(offset, 0xff)

		iter = cn.db.NewIterator(&util.Range{
			Start: cutset,
			Limit: offset,
		}, nil)

		for ok := iter.Last(); ok; ok = iter.Prev() {

			if limitNum < 1 {
				break
			}

			if bytes.Compare(iter.Key(), offset) >= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) < 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			limitSize -= int64(len(iter.Value()))
			if limitSize < 1 {
				break
			}

			limitNum -= 1
			values = append(values, bytes_clone(iter.Value()))
		}

	} else {

		cutset = append(cutset, 0xff)

		iter = cn.db.NewIterator(&util.Range{
			Start: offset,
			Limit: cutset,
		}, nil)

		for iter.Next() {

			if limitNum < 1 {
				break
			}

			if bytes.Compare(iter.Key(), offset) <= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) >= 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			limitSize -= int64(len(iter.Value()))
			if limitSize < 1 {
				break
			}

			limitNum -= 1
			values = append(values, bytes_clone(iter.Value()))
		}
	}

	iter.Release()

	if iter.Error() != nil {
		return iter.Error()
	}

	for _, bs := range values {
		if item, err := sko.ObjectItemDecode(bs); err == nil {
			rs.Items = append(rs.Items, item)
		}
	}

	if limitNum < 1 || limitSize < 1 {
		rs.Next = true
	}

	return nil
}

func (cn *SkoConn) objectQueryLogRange(rr *sko.ObjectReader, rs *sko.ObjectResult) error {

	var (
		offset    = t_ns_cat(ns_sko_log, uint64_to_bytes(rr.LogOffset))
		cutset    = t_ns_cat(ns_sko_log, []byte{0xff})
		limitNum  = rr.LimitNum
		limitSize = rr.LimitSize
	)

	if limitNum > sko.ObjectReaderLimitNumMax {
		limitNum = sko.ObjectReaderLimitNumMax
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = sko.ObjectReaderLimitSizeDef
	} else if limitSize > sko.ObjectReaderLimitSizeMax {
		limitSize = sko.ObjectReaderLimitSizeMax
	}

	var (
		tto  = uint64(time.Now().UnixNano()/1e6) - 3000
		iter = cn.db.NewIterator(&util.Range{
			Start: offset,
			Limit: cutset,
		}, nil)
	)

	for iter.Next() {

		if limitNum < 1 {
			break
		}

		if bytes.Compare(iter.Key(), offset) <= 0 {
			continue
		}

		if bytes.Compare(iter.Key(), cutset) >= 0 {
			break
		}

		if len(iter.Value()) < 2 {
			continue
		}

		meta, err := sko.ObjectMetaDecode(iter.Value())
		if err != nil || meta == nil {
			break
		}

		//
		if sko.AttrAllow(meta.Attrs, sko.ObjectMetaAttrDelete) {
			rs.Items = append(rs.Items, &sko.ObjectItem{
				Meta: meta,
			})
		} else {

			bs, err := cn.db.Get(t_ns_cat(ns_sko_data, meta.Key), nil)
			if err != nil {
				break
			}

			limitSize -= int64(len(bs))
			if limitSize < 1 {
				break
			}

			if item, err := sko.ObjectItemDecode(bs); err == nil {
				if item.Meta.Updated >= tto {
					break
				}
				rs.Items = append(rs.Items, item)
			}
		}

		limitNum -= 1
	}

	iter.Release()

	if iter.Error() != nil {
		return iter.Error()
	}

	if limitNum < 1 || limitSize < 1 {
		rs.Next = true
	}

	return nil
}

func (cn *SkoConn) NewReader(key []byte) *sko.ClientReader {
	return sko.NewClientReader(cn, key)
}

func (cn *SkoConn) NewWriter(key []byte, value interface{}) *sko.ClientWriter {
	return sko.NewClientWriter(cn, key, value)
}

func (cn *SkoConn) objectMetaGet(rr *sko.ObjectWriter) (*sko.ObjectMeta, error) {

	data, err := cn.db.Get(t_ns_cat(ns_sko_meta, rr.Meta.Key), nil)
	if err == nil {
		return sko.ObjectMetaDecode(data)
	} else {
		if err.Error() == ldbNotFound {
			err = nil
		}
	}

	return nil, err
}

func (cn *SkoConn) objectLogVersionSet(incr, set uint64) (uint64, error) {

	cn.skoLogMu.Lock()
	defer cn.skoLogMu.Unlock()

	if incr == 0 && set == 0 {
		return cn.skoLogOffset, nil
	}

	if cn.skoLogCutset <= 100 {

		if bs, err := cn.db.Get(skoKeySysLogCutset, nil); err != nil {
			if err.Error() != ldbNotFound {
				return 0, err
			}
		} else {
			if cn.skoLogCutset, err = strconv.ParseUint(string(bs), 10, 64); err != nil {
				return 0, err
			}
			if cn.skoLogOffset < cn.skoLogCutset {
				cn.skoLogOffset = cn.skoLogCutset
			}
		}
	}

	if cn.skoLogOffset < 100 {
		cn.skoLogOffset = 100
	}

	if set > 0 && set > cn.skoLogOffset {
		incr += (set - cn.skoLogOffset)
	}

	if incr > 0 {

		if (cn.skoLogOffset + incr) >= cn.skoLogCutset {

			cutset := cn.skoLogOffset + incr + 100

			if n := cutset % 100; n > 0 {
				cutset += n
			}

			if err := cn.db.Put(skoKeySysLogCutset,
				[]byte(strconv.FormatUint(cutset, 10)), nil); err != nil {
				return 0, err
			}

			cn.skoLogCutset = cutset
		}

		cn.skoLogOffset += incr
	}

	return cn.skoLogOffset, nil
}

func (cn *SkoConn) objectIncrSet(ns string, incr, set uint64) (uint64, error) {

	cn.skoIncrMu.Lock()
	defer cn.skoIncrMu.Unlock()

	if incr == 0 && set == 0 {
		return cn.skoIncrOffset, nil
	}

	if cn.skoIncrCutset <= 100 {

		if bs, err := cn.db.Get(skoKeySysIncrCutset(ns), nil); err != nil {
			if err.Error() != ldbNotFound {
				return 0, err
			}
		} else {
			if cn.skoIncrCutset, err = strconv.ParseUint(string(bs), 10, 64); err != nil {
				return 0, err
			}
			if cn.skoIncrOffset < cn.skoIncrCutset {
				cn.skoIncrOffset = cn.skoIncrCutset
			}
		}
	}

	if cn.skoIncrOffset < 100 {
		cn.skoIncrOffset = 100
	}

	if set > 0 && set > cn.skoIncrOffset {
		incr += (set - cn.skoIncrOffset)
	}

	if incr > 0 {

		if (cn.skoIncrOffset + incr) >= cn.skoIncrCutset {

			cutset := cn.skoIncrOffset + incr + 100

			if n := cutset % 100; n > 0 {
				cutset += n
			}

			if err := cn.db.Put(skoKeySysIncrCutset(ns),
				[]byte(strconv.FormatUint(cutset, 10)), nil); err != nil {
				return 0, err
			}

			cn.skoIncrCutset = cutset
		}

		cn.skoIncrOffset += incr
	}

	return cn.skoIncrOffset, nil
}
