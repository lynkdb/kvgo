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

package kvgo

import (
	"bytes"

	tsd2 "github.com/valuedig/apis/go/tsd/v2"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

type monitorStorage struct {
	tdb *dbTable
}

func (cn *Conn) monitorSetup() error {

	if cn.opts.Server.Bind == "" {
		return nil
	}

	if cn.monitor == nil {
		if tdb, ok := cn.tables["main"]; ok {
			monitor := tsd2.NewSampler()
			monitor.StorageSetup("kvgo", &monitorStorage{
				tdb: tdb,
			})
			cn.monitor = monitor
		}
	}
	return nil
}

func (it *monitorStorage) Put(key, value []byte) error {

	rr := kv2.NewObjectWriter(key, value).
		ExpireSet(10 * 86400 * 1000)

	bsMeta, bsData, err := rr.PutEncode()
	if err != nil {
		return err
	}

	batch := it.tdb.db.NewBatch()
	batch.Put(keyEncode(nsKeyData, rr.Meta.Key), bsData)
	batch.Put(keyExpireEncode(nsKeyTtl, rr.Meta.Expired, rr.Meta.Key), bsMeta)

	return batch.Commit()
}

func (it *monitorStorage) Scan(offset, cutset []byte) ([][]byte, error) {

	offset = keyEncode(nsKeyData, offset)
	cutset = keyEncode(nsKeyData, append(cutset, 0xff))

	var (
		values   = [][]byte{}
		limitNum = 5000
		iter     = it.tdb.db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})
	)

	for ok := iter.First(); ok; ok = iter.Next() {

		if limitNum < 1 {
			break
		}

		if bytes.Compare(iter.Key(), offset) <= 0 {
			continue
		}

		if bytes.Compare(iter.Key(), cutset) >= 0 {
			break
		}

		limitNum--
		if item, err := kv2.ObjectItemDecode(iter.Value()); err == nil {
			values = append(values, item.DataValue().Bytes())
		}
	}

	iter.Release()

	return values, nil
}
