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
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

func storagePebbleOpen(path string, opts *kv2.StorageOptions) (kv2.StorageEngine, error) {

	dir := filepath.Clean(path) + "_pebble"

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	opts = opts.Reset()

	ldbOpts := &pebble.Options{
		Cache: pebble.NewCache(int64(opts.WriteBufferSize) * int64(kv2.MiB)),
		// LBaseMaxBytes: int64(opts.MaxTableSize) * int64(kv2.MiB),
		MaxOpenFiles: opts.MaxOpenFiles,
	}

	// comp := pebble.NoCompression
	// if opts.TableCompressName == "snappy" {
	// 	comp = pebble.SnappyCompression
	// }

	// for i := 0; i < 1; i++ {
	// 	lops := pebble.LevelOptions{
	// 		TargetFileSize: int64(opts.MaxTableSize) * int64(kv2.MiB),
	// 		Compression:    comp,
	// 	}
	// 	ldbOpts.Levels = append(ldbOpts.Levels, lops)
	// }

	db, err := pebble.Open(dir, ldbOpts)
	if err != nil {
		return nil, err
	}

	return &pebbleStorageEngine{
		db: db,
	}, nil
}

type pebbleStorageEngine struct {
	db *pebble.DB
}

func (it *pebbleStorageEngine) Put(key, value []byte,
	opts *kv2.StorageWriteOptions) kv2.StorageResult {
	return newPebbleStorageResultError(it.db.Set(key, value, pebble.NoSync))
}

func (it *pebbleStorageEngine) Get(key []byte,
	opts *kv2.StorageReadOptions) kv2.StorageResult {
	value, closer, err := it.db.Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err == nil {
		return newPebbleStorageResult(bytesClone(value), nil)
	}
	return newPebbleStorageResultError(err)
}

func (it *pebbleStorageEngine) Delete(key []byte,
	opts *kv2.StorageDeleteOptions) kv2.StorageResult {
	return newPebbleStorageResultError(it.db.Delete(key, pebble.NoSync))
}

func (it *pebbleStorageEngine) NewBatch() kv2.StorageBatch {
	b := &pebbleStorageBatch{
		batch: it.db.NewBatch(),
	}
	b.batch.Reset()
	return b
}

func (it *pebbleStorageEngine) NewIterator(opts *kv2.StorageIteratorRange) kv2.StorageIterator {
	return newPebbleStorageIterator(it.db, opts)
}

func (it *pebbleStorageEngine) SizeOf(args []*kv2.StorageIteratorRange) ([]int64, error) {
	if len(args) == 0 {
		return nil, nil
	}
	rs := []int64{}
	for _, v := range args {
		if siz, err := it.db.EstimateDiskUsage(v.Start, v.Limit); err != nil {
			return nil, err
		} else {
			rs = append(rs, int64(siz))
		}
	}
	return rs, nil
}

func (it *pebbleStorageEngine) Close() error {
	return it.db.Close()
}

type pebbleStorageResult struct {
	bytes []byte
	err   error
}

func newPebbleStorageResult(bs []byte, err error) *pebbleStorageResult {
	return &pebbleStorageResult{
		bytes: bs,
		err:   err,
	}
}

func newPebbleStorageResultError(err error) *pebbleStorageResult {
	return &pebbleStorageResult{
		err: err,
	}
}

func (it *pebbleStorageResult) OK() bool {
	return it.err == nil
}

func (it *pebbleStorageResult) Error() error {
	return it.err
}

func (it *pebbleStorageResult) ErrorMessage() string {
	if it.err != nil {
		return it.err.Error()
	}
	return ""
}

func (it *pebbleStorageResult) NotFound() bool {
	return (it.err != nil && it.err == pebble.ErrNotFound)
}

func (it *pebbleStorageResult) Bytes() []byte {
	return it.bytes
}

func (it *pebbleStorageResult) Len() int {
	return len(it.bytes)
}

func (it *pebbleStorageResult) String() string {
	return string(it.bytes)
}

type pebbleStorageBatch struct {
	batch *pebble.Batch
}

func (it *pebbleStorageBatch) Put(key, value []byte) {
	it.batch.Set(key, value, pebble.NoSync)
}

func (it *pebbleStorageBatch) Delete(key []byte) {
	it.batch.Delete(key, pebble.NoSync)
}

func (it *pebbleStorageBatch) Len() int {
	return int(it.batch.Count())
}

func (it *pebbleStorageBatch) Reset() {
	it.batch.Reset()
}

func (it *pebbleStorageBatch) Commit() error {
	return it.batch.Commit(&pebble.WriteOptions{
		Sync: false,
	})
}

type pebbleStorageIterator struct {
	*pebble.Iterator
}

func newPebbleStorageIterator(db *pebble.DB,
	opts *kv2.StorageIteratorRange) *pebbleStorageIterator {
	return &pebbleStorageIterator{
		Iterator: db.NewIter(&pebble.IterOptions{
			LowerBound: opts.Start,
			UpperBound: opts.Limit,
		}),
	}
}

func (it *pebbleStorageIterator) Release() {
	it.Close()
}

func (it *pebbleStorageIterator) Seek(key []byte) bool {
	return it.SeekGE(key)
}
