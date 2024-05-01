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

//go:build enable_storage_goleveldb

package goleveldb

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	lerrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	literator "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

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
	return "v1"
}

func (it *driver) Open(opts *storage.Options) (storage.Conn, error) {

	mu.Lock()
	defer mu.Unlock()

	if opts == nil {
		return errors.New("storage.Options not setup")
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

	ldbOpts := &opt.Options{
		WriteBuffer:            opts.WriteBufferSize << 20,
		BlockCacheCapacity:     opts.BlockCacheSize << 20,
		CompactionTableSize:    opts.MaxTableSize << 20,
		OpenFilesCacheCapacity: opts.MaxOpenFiles,
		Filter:                 filter.NewBloomFilter(10),
	}

	if opts.Compression == "snappy" {
		ldbOpts.Compression = opt.SnappyCompression
	} else {
		ldbOpts.Compression = opt.NoCompression
	}

	db, err := leveldb.OpenFile(opts.DataDirectory, ldbOpts)
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
	db     *leveldb.DB
	opts   *storage.Options
	closed bool
}

var _ storage.Conn = &engine{}

func (it *engine) Get(key []byte,
	opts *storage.ReadOptions) storage.Result {
	return newResult(it.db.Get(key, nil))
}

func (it *engine) NewIterator(opts *storage.IterOptions) storage.Iterator {
	return &iterator{
		iter: it.db.NewIterator(&util.Range{
			Start: opts.LowerKey,
			Limit: opts.UpperKey,
		}, nil),
	}
}

func (it *engine) Put(key, value []byte,
	opts *storage.WriteOptions) storage.Result {
	if opts == nil || !opts.Sync {
		return newResult(nil, it.db.Put(key, value, nil))
	}
	return newResult(nil, it.db.Put(key, value, &opt.WriteOptions{Sync: true}))
}

func (it *engine) Delete(key []byte,
	opts *storage.WriteOptions) storage.Result {
	if opts == nil || !opts.Sync {
		return newResult(nil, it.db.Delete(key, nil))
	}
	return newResult(nil, it.db.Delete(key, &opt.WriteOptions{Sync: true}))
}

func (it *engine) DeleteRange(
	lowerKey, upperKey []byte,
	opts *storage.WriteOptions,
) storage.Result {
	return newResult(nil, errors.New("un-support"))
}

func (it *engine) ExpCompact(
	lowerKey, upperKey []byte,
) error {
	return errors.New("un-support")
}

func (it *engine) NewBatch() storage.WriteBatch {
	b := &writeBatch{
		batch: new(leveldb.Batch),
		db:    it.db,
	}
	b.batch.Reset()
	return b
}

func (it *engine) SizeOf(args []*storage.IterOptions) ([]int64, error) {
	opts := []util.Range{}
	for _, v := range args {
		opts = append(opts, util.Range{Start: v.LowerKey, Limit: v.UpperKey})
	}
	return it.db.SizeOf(opts)
}

func (it *engine) Info() *storage.Info {
	return &storage.Info{
		Dir:     it.opts.DataDirectory,
		Options: map[string]string{},
	}
}

func (it *engine) Flush() error {
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
	return (it.err != nil && it.err.Error() == lerrors.ErrNotFound.Error())
}

func (it *result) Bytes() []byte {
	return it.bytes
}

// func (it *result) Len() int {
// 	return len(it.bytes)
// }

type writeBatch struct {
	batch *leveldb.Batch
	db    *leveldb.DB
}

func (it *writeBatch) Put(key, value []byte) {
	it.batch.Put(key, value)
}

func (it *writeBatch) Delete(key []byte) {
	it.batch.Delete(key)
}

func (it *writeBatch) Len() int {
	return it.batch.Len()
}

func (it *writeBatch) Clear() {
	it.batch.Reset()
}

func (it *writeBatch) Apply() storage.Result {
	return newResult(nil, it.db.Write(it.batch, nil))
}

type iterator struct {
	iter literator.Iterator
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
	return it.iter.Seek(key)
}

func (it *iterator) Prev() bool {
	return it.iter.Prev()
}

func (it *iterator) Next() bool {
	return it.iter.Next()
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
	it.iter.Release()
}
