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

	"github.com/lynkdb/iomix/sko"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (cn *Conn) objectMetaGet(rr *sko.ObjectWriter) (*sko.ObjectMeta, error) {

	data, err := cn.db.Get(t_ns_cat(ns_sko_meta, rr.Meta.Key), nil)
	if err == nil {
		return sko.ObjectMetaDecode(data)
	} else {
		if err.Error() == "leveldb: not found" {
			err = nil
		}
	}

	return nil, err
}

func (cn *Conn) ObjectDel(rr *sko.ObjectWriter) *sko.ObjectResult {

	if err := rr.DelValid(); err != nil {
		return sko.NewObjectResultClientError(err)
	}

	meta, err := cn.objectMetaGet(rr)
	if meta == nil && err != nil {
		return sko.NewObjectResultServerError(err)
	}

	if meta != nil {

		batch := new(leveldb.Batch)
		batch.Delete(t_ns_cat(ns_sko_meta, rr.Meta.Key))
		batch.Delete(t_ns_cat(ns_sko_data, rr.Meta.Key))
		if rr.Meta.Version > 0 {
			batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(rr.Meta.Version)))
		}
		err = cn.db.Write(batch, nil)
	}

	return sko.NewObjectResult(0, err)
}

func (cn *Conn) ObjectPut(rr *sko.ObjectWriter) *sko.ObjectResult {

	if err := rr.PutValid(); err != nil {
		return sko.NewObjectResultClientError(err)
	}

	meta, err := cn.objectMetaGet(rr)
	if meta == nil && err != nil {
		return sko.NewObjectResultServerError(err)
	}

	if meta != nil &&
		meta.DataCheck == rr.Meta.DataCheck {
		return sko.NewObjectResultOK()
	}

	if meta != nil && meta.Created > 0 {
		rr.Meta.Created = meta.Created
	} else {
		rr.Meta.Created = rr.Meta.Updated
	}

	bsMeta, bsData, err := rr.PutEncode()
	if err == nil {

		batch := new(leveldb.Batch)

		batch.Put(t_ns_cat(ns_sko_meta, rr.Meta.Key), bsMeta)
		batch.Put(t_ns_cat(ns_sko_data, rr.Meta.Key), bsData)

		if rr.Meta.Version > 0 {
			batch.Put(t_ns_cat(ns_sko_log, uint64_to_bytes(rr.Meta.Version)), bsMeta)
		}

		if rr.Meta.Expired > 0 {
			batch.Put(keyExpireEncode(ns_sko_ttl, rr.Meta.Expired, rr.Meta.Key), bsMeta)
		}

		if meta != nil && meta.Version < rr.Meta.Version {
			batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
		}

		err = cn.db.Write(batch, nil)
	}

	if err != nil {
		return sko.NewObjectResultServerError(err)
	}

	return sko.NewObjectResultOK()
}

func (cn *Conn) ObjectQuery(rr *sko.ObjectReader) *sko.ObjectResult {

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

				if err.Error() != "leveldb: not found" {
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

	} else {

		rs.StatusMessage(sko.ResultClientError, "invalid mode")
	}

	if rs.Status == 0 {
		rs.Status = sko.ResultOK
	}

	return rs
}

func (cn *Conn) objectQueryKeyRange(rr *sko.ObjectReader, rs *sko.ObjectResult) error {

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

func (cn *Conn) NewObjectWriter(key []byte) *sko.ObjectWriter {
	return sko.NewObjectWriter(key)
}

func (cn *Conn) NewObjectReader() *sko.ObjectReader {
	return sko.NewObjectReader()
}
