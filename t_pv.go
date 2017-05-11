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
	"path/filepath"
	"strings"
	"sync"

	"code.hooto.com/lynkdb/iomix/skv"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	t_pv_meta_locker       sync.Mutex
	t_pv_grpstatus_locker  sync.Mutex
	t_pv_event_handler     skv.PvEventHandler
	t_pv_write_options_def = &skv.PvWriteOptions{}
)

func (cn *Conn) PvNew(path string, value interface{}, opts *skv.PvWriteOptions) *skv.Result {

	db_value, err := skv.ValueEncode(value, nil)
	if err != nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	if opts == nil {
		opts = t_pv_write_options_def
	}

	return cn.RawNew(pv_path_parse(path).entry_index(), db_value, opts.Ttl)
}

func (cn *Conn) PvDel(path string, opts *skv.PvWriteOptions) *skv.Result {

	var (
		db_key = pv_path_parse(path).entry_index()
	)

	rs := cn.RawGet(db_key)
	if rs.Status == skv.ResultOK {

		if rs = cn.RawDel(db_key); rs.Status != skv.ResultOK {
			return rs
		}
	}

	return rs
}

func (cn *Conn) PvPut(path string, value interface{}, opts *skv.PvWriteOptions) *skv.Result {

	var (
		db_key = pv_path_parse(path).entry_index()
	)

	db_value, err := skv.ValueEncode(value, nil)
	if err != nil {
		return skv.NewResult(skv.ResultBadArgument)
	}

	if opts == nil {
		opts = t_pv_write_options_def
	}

	if opts.PrevValue != nil {

		pdb_value, err := skv.ValueEncode(opts.PrevValue, nil)
		if err != nil {
			return skv.NewResult(skv.ResultBadArgument)
		}

		if rs := cn.RawGet(db_key); rs.OK() {

			if bytes.Compare(rs.Bytes(), pdb_value) != 0 {
				return skv.NewResult(skv.ResultBadArgument)
			}
		}
	}

	return cn.RawPut(db_key, db_value, opts.Ttl)
}

func (cn *Conn) PvGet(path string) *skv.Result {
	return cn.RawGet(pv_path_parse(path).entry_index())
}

func (cn *Conn) PvScan(fold, offset, cutset string, limit int) *skv.Result {

	var (
		prefix = pv_path_fold_index(fold)
		prelen = len(prefix)
		off    = append(prefix, []byte(offset)...)
		cut    = append(prefix, []byte(cutset)...)
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

func (cn *Conn) PvRevScan(fold, offset, cutset string, limit int) *skv.Result {

	var (
		prefix = pv_path_fold_index(fold)
		prelen = len(prefix)
		off    = append(prefix, []byte(offset)...)
		cut    = append(prefix, []byte(cutset)...)
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

//
type pv_path struct {
	Fold string
	Name string
}

func (p *pv_path) entry_index() []byte {
	return append([]byte{ns_pv, uint8(len(p.Fold))}, append([]byte(p.Fold), []byte(p.Name)...)...)
}

func pv_path_parse(path string) *pv_path {

	p := &pv_path{}

	is_fold := false
	if len(path) > 0 && path[len(path)-1] == '/' {
		is_fold = true
	}

	path = pvpath_clean(path)

	if is_fold {
		p.Fold, p.Name = path, ""
	} else {
		if i := strings.LastIndex(path, "/"); i > 0 {
			p.Fold, p.Name = path[:i], path[i+1:]
		} else {
			p.Fold, p.Name = "", path
		}
	}

	return p
}

func pv_path_fold_index(fold string) []byte {
	fold = pvpath_clean(fold)
	return append([]byte{ns_pv, uint8(len(fold))}, []byte(fold)...)
}

func pvpath_clean(path string) string {
	return strings.Trim(strings.Trim(filepath.Clean(path), "/"), ".")
}
