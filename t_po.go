// Copyright 2015 lynkdb Authors, All rights reserved.
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
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strings"

	"code.hooto.com/lynkdb/iomix/skv"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	t_po_write_options_def = &skv.PathWriteOptions{}
)

func (cn *Conn) PoNew(path string, key, value interface{}, opts *skv.PathWriteOptions) *skv.Result {

	db_key := po_pathkey_enc(path, key)
	if db_key == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	db_value, err := skv.ValueEncode(value, nil)
	if err != nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	if opts == nil {
		opts = t_po_write_options_def
	}

	return cn.RawNew(db_key, db_value, opts.Ttl)
}

func (cn *Conn) PoDel(path string, key interface{}, opts *skv.PathWriteOptions) *skv.Result {

	db_key := po_pathkey_enc(path, key)
	if db_key == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	rs := cn.RawGet(db_key)
	if rs.Status == skv.ResultOK {

		if rs = cn.RawDel(db_key); rs.Status != skv.ResultOK {
			return rs
		}
	}

	return rs
}

func (cn *Conn) PoPut(path string, key, value interface{}, opts *skv.PathWriteOptions) *skv.Result {

	db_key := po_pathkey_enc(path, key)
	if db_key == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	db_value, err := skv.ValueEncode(value, nil)
	if err != nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	if opts == nil {
		opts = t_po_write_options_def
	}

	if opts.PrevValue != nil {

		db_value_prev, err := skv.ValueEncode(opts.PrevValue, nil)
		if err != nil {
			return skv.NewResult(skv.ResultBadArgument)
		}

		if rs := cn.RawGet(db_key); rs.OK() {

			if bytes.Compare(rs.Bytes(), db_value_prev) != 0 {
				return skv.NewResult(skv.ResultBadArgument)
			}
		}
	}

	return cn.RawPut(db_key, db_value, opts.Ttl)
}

func (cn *Conn) PoGet(path string, key interface{}) *skv.Result {
	db_key := po_pathkey_enc(path, key)
	if db_key == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}
	return cn.RawGet(db_key)
}

func (cn *Conn) PoScan(path string, offset, cutset interface{}, limit int) *skv.Result {

	pe := po_path_enc(path)
	if pe == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	var (
		prelen = len(pe)
		off    = append(pe, po_key_enc(offset)...)
		cut    = append(pe, po_key_enc(cutset)...)
		rs     = skv.NewResult(0)
	)

	for i := prelen; i < 200; i++ {
		cut = append(cut, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	} else if limit < 1 {
		limit = 1
	}

	iter := cn.db.NewIterator(&util.Range{
		Start: off,
		Limit: cut,
	}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) <= prelen {
			continue
		}

		if len(iter.Value()) < 2 {
			continue
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()[prelen:]))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func (cn *Conn) PoRevScan(fold string, offset, cutset interface{}, limit int) *skv.Result {

	pe := po_path_enc(fold)
	if pe == nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	var (
		prelen = len(pe)
		off    = append(pe, po_key_enc(offset)...)
		cut    = append(pe, po_key_enc(cutset)...)
		rs     = skv.NewResult(0)
	)

	for i := len(off); i < 200; i++ {
		off = append(off, 0x00)
	}

	for i := len(cut); i < 200; i++ {
		cut = append(cut, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
	} else if limit < 1 {
		limit = 1
	}

	iter := cn.db.NewIterator(&util.Range{
		Start: off,
		Limit: cut,
	}, nil)

	for ok := iter.Last(); ok; ok = iter.Prev() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) <= prelen {
			continue
		}

		if len(iter.Value()) < 2 {
			continue
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()[prelen:]))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func po_pathkey_enc(path string, key interface{}) []byte {

	ke := po_key_enc(key)
	if len(ke) == 0 {
		return nil
	}

	pe := po_path_enc(path)
	if pe == nil {
		return nil
	}

	return append(pe, ke...)
}

func po_path_enc(path string) []byte {

	if path = strings.Trim(strings.Trim(filepath.Clean(path), "/"), "."); len(path) > 0 {
		return append([]byte{ns_path, uint8(len(path))}, []byte(path)...)
	}

	return nil
}

func po_key_enc(key interface{}) []byte {

	switch key.(type) {

	case uint32:
		ke := make([]byte, 4)
		binary.BigEndian.PutUint32(ke, key.(uint32))
		return ke

	case uint64:
		ke := make([]byte, 8)
		binary.BigEndian.PutUint64(ke, key.(uint64))
		return ke

	case uint16:
		ke := make([]byte, 2)
		binary.BigEndian.PutUint16(ke, key.(uint16))
		return ke

	case []byte:
		if ke := key.([]byte); len(ke) <= 32 {
			return ke
		}

	case string:
		if ke, err := hex.DecodeString(key.(string)); err == nil {
			return ke
		}
	}

	return []byte{}
}
