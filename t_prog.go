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
	"errors"
	"sync"

	"github.com/lynkdb/iomix/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	t_prog_incr_mu           sync.Mutex
	t_prog_write_options_def = &skv.ProgWriteOptions{}
)

func (cn *Conn) ProgNew(key skv.ProgKey, val skv.ProgValue, opts *skv.ProgWriteOptions) *skv.Result {

	if opts == nil {
		opts = t_prog_write_options_def
	}

	opts.Actions = opts.Actions | skv.ProgOpCreate

	return cn.ProgPut(key, val, opts)
}

func (cn *Conn) ProgPut(key skv.ProgKey, val skv.ProgValue, opts *skv.ProgWriteOptions) *skv.Result {

	if !key.Valid() || !val.Valid() {
		return skv.NewResult(skv.ResultBadArgument)
	}

	if opts == nil {
		opts = t_prog_write_options_def
	}

	var (
		p_rs      *skv.Result
		p_meta    *skv.ValueMeta
		prev_diff = true
	)

	if opts.OpAllow(skv.ProgOpCreate) {
		if rs := cn.RawGet(key.Encode(ns_prog_def)); rs.OK() {
			return skv.NewResult(0)
		} else {
			p_rs, p_meta = rs, rs.Meta()
		}
	}

	if i, entry := key.LastEntry(); entry != nil {

		switch entry.Type {

		case skv.ProgKeyEntryIncr:
			if i < 1 {
				return skv.NewResult(skv.ResultBadArgument)
			}
			pmeta := key.EncodeIndex(ns_prog_def, i-1)
			if len(pmeta) < 1 {
				return skv.NewResult(skv.ResultBadArgument)
			}
			kiv, err := cn.progExtIncrby(pmeta, 1)
			if err != nil {
				return skv.NewResult(skv.ResultBadArgument)
			}
			if err := key.Set(i, kiv); err != nil {
				return skv.NewResult(skv.ResultBadArgument)
			}
			prev_diff = false

		case skv.ProgKeyEntryBytes:
		case skv.ProgKeyEntryUint:

		default:
			return skv.NewResult(skv.ResultBadArgument)
		}
	}

	if prev_diff && (opts.PrevSum > 0 || opts.OpAllow(skv.ProgOpFoldMeta)) {
		if p_rs == nil {
			if rs := cn.RawGet(key.Encode(ns_prog_def)); rs.OK() {
				p_rs, p_meta = rs, rs.Meta()
			}
		}
	}

	if opts.PrevSum > 0 && p_rs != nil {
		if p_rs.Crc32() != opts.PrevSum {
			return skv.NewResult(skv.ResultBadArgument)
		}
	}

	if opts.OpAllow(skv.ProgOpMetaSum) {
		val.Meta().Sum = 1
	}
	if opts.OpAllow(skv.ProgOpMetaSize) {
		val.Meta().Size = 1
	}

	batch := new(leveldb.Batch)

	if nsec := opts.ExpiredUnixNano(); nsec > 0 {
		val.Meta().Expired = nsec
		ttl_key := t_ns_cat(ns_prog_ttl,
			append(uint64_to_bytes(nsec), key.Encode(ns_prog_def)...))
		batch.Put(ttl_key, []byte{0x00})
	}

	if opts.OpAllow(skv.ProgOpFoldMeta) {
		fmeta := cn.RawGet(key.EncodeFoldMeta(ns_prog_def)).Meta()
		if fmeta == nil {
			fmeta = &skv.ValueMeta{}
		}
		if p_rs == nil || p_meta == nil || p_meta.Num == 0 {
			fmeta.Num++
			fmeta.Size += uint64(val.ValueSize())
		} else {
			if si := (val.ValueSize() - p_rs.ValueSize()); si > 0 {
				fmeta.Size += uint64(si)
			} else if fmeta.Size > uint64(-si) {
				fmeta.Size -= uint64(-si)
			} else {
				fmeta.Size = 0
			}
		}
		if bs := fmeta.Encode(); len(bs) > 1 {
			cn.RawPut(key.EncodeFoldMeta(ns_prog_def), bs, 0)
		}
		val.Meta().Num = 1
	}

	batch.Put(key.Encode(ns_prog_def), val.Encode())
	if err := cn.db.Write(batch, nil); err != nil {
		return skv.NewResultError(skv.ResultServerError, err.Error())
	}

	return skv.NewResult(0)
}

func (cn *Conn) ProgGet(key skv.ProgKey) *skv.Result {
	if len(key.Encode(ns_prog_def)) == 0 {
		return skv.NewResult(skv.ResultBadArgument)
	}
	return cn.RawGet(key.Encode(ns_prog_def))
}

func (cn *Conn) ProgDel(key skv.ProgKey, opts *skv.ProgWriteOptions) *skv.Result {

	if len(key.Encode(ns_prog_def)) == 0 {
		return skv.NewResult(skv.ResultBadArgument)
	}

	rs := cn.RawGet(key.Encode(ns_prog_def))
	if rs.OK() {
		if meta := rs.Meta(); meta != nil && meta.Num == 1 {

			if fmeta := cn.RawGet(key.EncodeFoldMeta(ns_prog_def)).Meta(); fmeta != nil {
				if fmeta.Size > uint64(rs.ValueSize()) {
					fmeta.Size -= uint64(rs.ValueSize())
				} else {
					fmeta.Size = 0
				}
				if fmeta.Num <= 1 {
					cn.RawDel(key.EncodeFoldMeta(ns_prog_def))
				} else {
					fmeta.Num--
					if bs := fmeta.Encode(); len(bs) > 1 {
						cn.RawPut(key.EncodeFoldMeta(ns_prog_def), bs, 0)
					}
				}
			}
		}

		rs = cn.RawDel(key.Encode(ns_prog_def))
	}

	return rs
}

func (cn *Conn) ProgScan(offset, cutset skv.ProgKey, limit int) *skv.Result {

	var (
		plen = offset.FoldLen()
		off  = offset.Encode(ns_prog_def)
		cut  = cutset.Encode(ns_prog_def)
		rs   = skv.NewResult(0)
	)

	for i := len(cut); i < 200; i += 4 {
		cut = append(cut, []byte{0xff, 0xff, 0xff, 0xff}...)
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

		if len(iter.Key()) <= plen || len(iter.Value()) < 2 {
			continue
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()[plen:]))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func (cn *Conn) ProgRevScan(offset, cutset skv.ProgKey, limit int) *skv.Result {

	var (
		plen = offset.FoldLen()
		off  = offset.Encode(ns_prog_def)
		cut  = cutset.Encode(ns_prog_def)
		rs   = skv.NewResult(0)
	)

	for i := len(off); i < 200; i += 4 {
		off = append(off, []byte{0x00, 0x00, 0x00, 0x00}...)
	}

	for i := len(cut); i < 200; i += 4 {
		cut = append(cut, []byte{0xff, 0xff, 0xff, 0xff}...)
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

		if len(iter.Key()) <= plen || len(iter.Value()) < 2 {
			continue
		}

		rs.Data = append(rs.Data, bytes_clone(iter.Key()[plen:]))
		rs.Data = append(rs.Data, bytes_clone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		return skv.NewResultError(skv.ResultServerError, iter.Error().Error())
	}

	return rs
}

func (cn *Conn) progExtIncrby(key []byte, incr int64) (uint64, error) {

	if incr < 1 {
		return 0, errors.New("BadArgument::INCR")
	}

	key_enc := append(ns_prog_x_incr, key...)

	t_prog_incr_mu.Lock()
	defer t_prog_incr_mu.Unlock()

	if rs := cn.RawGet(key_enc); !rs.OK() {
		if !rs.NotFound() {
			return 0, errors.New("server error")
		}
	} else {
		incr += rs.Int64()
		if incr < 1 {
			return 0, errors.New("BadArgument::INCR")
		}
	}

	if value_enc, err := skv.ValueEncode(incr, nil); err == nil {
		if rs := cn.RawPut(key_enc, value_enc, 0); rs.OK() {
			return uint64(incr), nil
		}
	}

	return 0, errors.New("server error")
}
