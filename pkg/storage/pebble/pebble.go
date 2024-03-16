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

package pebble

import (
	"errors"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"

	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

var (
	mu    sync.Mutex
	conns = map[string]storage.Conn{}
)

func init() {
	storage.Register(new(driver))
}

type driver struct{}

func (it *driver) Name() string {
	return "v2"
}

func (it *driver) Open(opts *storage.Options) (storage.Conn, error) {

	mu.Lock()
	defer mu.Unlock()

	if opts == nil {
		return nil, errors.New("storage.Options not setup")
	}
	opts.Reset()

	log.Printf("storage open %s", opts.DataDirectory)

	conn, ok := conns[opts.DataDirectory]
	if ok {
		return conn, nil
	}

	if err := os.MkdirAll(opts.DataDirectory, 0750); err != nil {
		return nil, err
	}

	ldbOpts := &pebble.Options{
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20,
		Levels:                make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions: func() int {
			n := runtime.NumCPU() / 8
			if n < 1 {
				n = 1
			} else if n > 8 {
				n = 8
			}
			return n
		},
		MemTableSize:                uint64(opts.WriteBufferSize) << 20,
		MemTableStopWritesThreshold: 4,
		Cache:                       pebble.NewCache(int64(opts.BlockCacheSize << 20)),
		MaxOpenFiles:                opts.MaxOpenFiles,
	}

	comp := pebble.NoCompression
	if opts.Compression != "none" {
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

	db, err := pebble.Open(opts.DataDirectory, ldbOpts)
	if err != nil {
		return nil, err
	}

	conn = &engine{
		db:   db,
		opts: opts,
	}
	conns[opts.DataDirectory] = conn

	return conn, nil
}

type engine struct {
	mu     sync.Mutex
	db     *pebble.DB
	opts   *storage.Options
	closed bool
}

var _ storage.Conn = &engine{}

func (it *engine) Get(
	key []byte,
	opts *storage.ReadOptions,
) storage.Result {
	value, closer, err := it.db.Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err == nil {
		return newResult(bytesClone(value), nil)
	}
	return newResult(nil, err)
}

func (it *engine) NewIterator(
	opts *storage.IterOptions,
) (storage.Iterator, error) {
	iter, err := it.db.NewIter(&pebble.IterOptions{
		LowerBound: opts.LowerKey,
		UpperBound: opts.UpperKey,
	})
	if err != nil {
		return nil, err
	}
	return &iterator{
		iter: iter,
	}, nil
}

func (it *engine) Put(
	key, value []byte,
	opts *storage.WriteOptions,
) storage.Result {
	if opts == nil || !opts.Sync {
		return newResult(nil, it.db.Set(key, value, pebble.NoSync))
	}
	return newResult(nil, it.db.Set(key, value, pebble.Sync))
}

func (it *engine) Delete(
	key []byte,
	opts *storage.WriteOptions,
) storage.Result {
	if opts == nil || !opts.Sync {
		return newResult(nil, it.db.Delete(key, pebble.NoSync))
	}
	return newResult(nil, it.db.Delete(key, pebble.Sync))
}

func (it *engine) DeleteRange(
	lowerKey, upperKey []byte,
	opts *storage.WriteOptions,
) storage.Result {
	if opts == nil || !opts.Sync {
		return newResult(nil, it.db.DeleteRange(lowerKey, upperKey, pebble.NoSync))
	}
	return newResult(nil, it.db.DeleteRange(lowerKey, upperKey, pebble.Sync))
}

func (it *engine) ExpCompact(
	lowerKey, upperKey []byte,
) error {
	return it.db.Compact(lowerKey, upperKey, false)
}

func (it *engine) NewBatch() storage.WriteBatch {
	b := &writeBatch{
		batch: it.db.NewBatch(),
	}
	b.batch.Reset()
	return b
}

func (it *engine) SizeOf(
	args []*storage.IterOptions,
) ([]int64, error) {
	rs := make([]int64, len(args))
	for i, v := range args {
		if siz, err := it.db.EstimateDiskUsage(v.LowerKey, v.UpperKey); err != nil {
			return rs, err
		} else {
			rs[i] = int64(siz)
		}
	}
	return rs, nil
}

func (it *engine) Flush() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if !it.closed {
		return it.db.Flush()
	}
	return nil
}

func (it *engine) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if !it.closed {
		it.closed = true
		delete(conns, it.opts.DataDirectory)
		return it.db.Close()
	}
	return nil
}

func newResult(bs []byte, err error) *result {
	return &result{
		bytes: bs,
		err:   err,
	}
}

type result struct {
	bytes []byte
	err   error
}

var _ storage.Result = &result{}

func (it *result) OK() bool {
	return it.err == nil
}

func (it *result) Error() error {
	return it.err
}

func (it *result) ErrorMessage() string {
	if it.err != nil {
		return it.err.Error()
	}
	return ""
}

func (it *result) NotFound() bool {
	return (it.err != nil && it.err.Error() == pebble.ErrNotFound.Error())
}

func (it *result) Bytes() []byte {
	return it.bytes
}

// func (it *result) Len() int {
// 	return len(it.bytes)
// }

type writeBatch struct {
	batch *pebble.Batch
}

func (it *writeBatch) Put(key, value []byte) {
	it.batch.Set(key, value, pebble.NoSync)
}

func (it *writeBatch) Delete(key []byte) {
	it.batch.Delete(key, pebble.NoSync)
}

func (it *writeBatch) Len() int {
	return int(it.batch.Count())
}

func (it *writeBatch) Clear() {
	it.batch.Reset()
}

func (it *writeBatch) Apply(opts *storage.WriteOptions) storage.Result {
	if opts != nil && opts.Sync {
		return newResult(nil, it.batch.Commit(&pebble.WriteOptions{
			Sync: true,
		}))
	}
	return newResult(nil, it.batch.Commit(&pebble.WriteOptions{
		Sync: false,
	}))
}

type iterator struct {
	iter *pebble.Iterator
}

func (it *iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *iterator) SeekToFirst() bool {
	return it.iter.First()
}

func (it *iterator) SeekToLast() bool {
	return it.iter.Last()
}

func (it *iterator) Seek(key []byte) bool {
	return it.iter.SeekGE(key)
}

func (it *iterator) Next() bool {
	return it.iter.Next()
}

func (it *iterator) Prev() bool {
	return it.iter.Prev()
}

func (it *iterator) Key() []byte {
	return it.iter.Key()
}

func (it *iterator) Value() []byte {
	return it.iter.Value()
}

func (it *iterator) Error() error {
	return it.iter.Error()
}

func (it *iterator) Release() {
	it.iter.Close()
}

func bytesClone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
