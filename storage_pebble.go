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
	"github.com/cockroachdb/pebble/bloom"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

func StoragePebbleOpen(path string, opts *kv2.StorageOptions) (kv2.StorageEngine, error) {

	dir := filepath.Clean(path)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	if opts == nil {
		opts = &kv2.StorageOptions{}
	}
	opts = opts.Reset()

	ldbOpts := &pebble.Options{
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20,
		Levels:                make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions: func() int {
			return 1
		},
		MemTableSize:                uint64(opts.WriteBufferSize) << 20,
		MemTableStopWritesThreshold: 4,
		Cache:                       pebble.NewCache(int64(opts.BlockCacheSize << 20)),
		MaxOpenFiles:                opts.MaxOpenFiles,
	}

	// ldbOpts.Experimental.DeleteRangeFlushDelay = 10 * time.Second
	// ldbOpts.Experimental.MinDeletionRate = 128 << 20 // 128 MB
	// ldbOpts.Experimental.ReadSamplingMultiplier = -1

	comp := pebble.NoCompression
	if opts.TableCompressName != "none" {
		comp = pebble.SnappyCompression
	}

	for i := 0; i < len(ldbOpts.Levels); i++ {
		l := &ldbOpts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = ldbOpts.Levels[i-1].TargetFileSize * 2
		} else {
			l.TargetFileSize = 2 << 20
		}
		l.EnsureDefaults()
		l.Compression = comp
	}

	ldbOpts.Levels[6].FilterPolicy = nil

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
	iter, _ := db.NewIter(&pebble.IterOptions{
		LowerBound: opts.Start,
		UpperBound: opts.Limit,
	})
	return &pebbleStorageIterator{
		Iterator: iter,
	}
}

func (it *pebbleStorageIterator) Release() {
	it.Close()
}

func (it *pebbleStorageIterator) Seek(key []byte) bool {
	return it.SeekGE(key)
}
