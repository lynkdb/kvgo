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

package storage

import (
	"errors"
)

// Options holds the optional parameters for the DB storage engine
type Options struct {
	// DataDirectory defines filepath of data saved to disk.
	DataDirectory string `toml:"data_directory" json:"data_directory"`

	// WriteBufferSize defines maximum memory size of the journal before flushed to disk.
	//
	// The default value is 8 MiB.
	WriteBufferSize uint64 `toml:"write_buffer_size" json:"write_buffer_size" desc:"in MiB, default to 8"`

	// BlockCacheSize defines the capacity of the 'sorted table' block caching.
	//
	// The default value is 8 MiB.
	BlockCacheSize int `toml:"block_cache_size" json:"block_cache_size" desc:"in MiB, default to 8"`

	// MaxTableSize limits size of 'sorted table' that compaction generates.
	//
	// The default value is 8 MiB.
	MaxTableSize int `toml:"max_table_size" json:"max_table_size" desc:"in MiB, default to 8"`

	// MaxOpenFiles defines the capacity of the open files caching.
	//
	// The default value is 500.
	MaxOpenFiles int `toml:"max_open_files" json:"max_open_files" desc:"default to 500"`

	// Compression defines the 'sorted table' block compression to use.
	//
	// The default value is 'snappy'.
	Compression string `toml:"compression" json:"compression" desc:"default to snappy"`
}

type ReadOptions struct{}

type WriteOptions struct {
	Sync bool
}

// IterOptions is a key range.
type IterOptions struct {
	LowerKey []byte
	UpperKey []byte
}

// Result defines DB reply methods.
type Result interface {
	// Returns true iff the status indicates success.
	OK() bool

	// Bytes returns error.
	Error() error

	// Returns true if the key not found.
	NotFound() bool

	ErrorMessage() string

	// Bytes returns data in bytes.
	Bytes() []byte
}

// type ResultSet interface {
// 	OK() bool
// 	Error() bool
// 	NotFound() bool
//
// 	Len() int
// 	Next() Result
// 	NextResultSet() bool
// }

type Reader interface {

	// Get gets the value for the given key.
	Get(key []byte, opts *ReadOptions) Result

	// NewIterator returns an iterator for the latest snapshot of the DB.
	NewIterator(opts *IterOptions) (Iterator, error)
}

type Writer interface {
	// Put sets the value for the given key.
	Put(key, value []byte, opts *WriteOptions) Result

	// Delete deletes the value for the given key.
	Delete(key []byte, opts *WriteOptions) Result

	// DeleteRange deletes all of the point keys (and values) in the range
	// [lower, upper)
	DeleteRange(lowerKey, upperKey []byte, opts *WriteOptions) Result

	// EXP ...
	ExpCompact(lowerKey, upperKey []byte) error

	// NewBatch returns a new empty batch.
	NewBatch() WriteBatch
}

// WriteBatch defines a write batch methods.
type WriteBatch interface {
	// Put appends 'put operation' of the given key/value pair to the batch.
	Put(key, value []byte)

	// Delete appends 'delete operation' of the given key to the batch.
	Delete(key []byte)

	// Len returns number of records in the batch.
	Len() int

	// Clear all updates buffered in this batch.
	Clear()

	// Apply applies the batch to its parent writer.
	Apply(opts *WriteOptions) Result
}

// Conn is a connection to a database. It is not used concurrently by multiple goroutines.
type Conn interface {
	Reader
	Writer

	Flush() error

	// SizeOf calculates approximate sizes of the given key ranges.
	SizeOf(args []*IterOptions) ([]int64, error)

	// Close closes the DB. This will also releases any outstanding snapshot,
	// abort any in-flight compaction and discard open transaction.
	Close() error
}

// Iterator is the interface that wraps iterator methods.
type Iterator interface {
	// Valid iterator is either positioned at a key/value pair, or
	// not valid.  This method returns true iff the iterator is valid.
	Valid() bool

	// SeekToFirst moves the iterator to the first key/value pair. If the iterator
	// only contains one key/value pair then first and last would moves
	// to the same key/value pair.
	// It returns whether such pair exist.
	SeekToFirst() bool

	// SeekToLast moves the iterator to the last key/value pair. If the iterator
	// only contains one key/value pair then first and last would moves
	// to the same key/value pair.
	// It returns whether such pair exist.
	SeekToLast() bool

	// Seek moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	// It returns whether such pair exist.
	//
	// It is safe to modify the contents of the argument after Seek returns.
	Seek(key []byte) bool

	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Prev moves the iterator to the previous key/value pair.
	// It returns false if the iterator is exhausted.
	Prev() bool

	// Key returns the key of the current key/value pair
	Key() []byte

	// Value returns the value of the current key/value pair
	Value() []byte

	// Error returns any accumulated error.
	Error() error

	// Release releases associated resources. Release should always success
	// and can be called multiple times without causing error.
	Release()
}

func (it *Options) Valid() error {
	if it.DataDirectory == "" {
		return errors.New("DataDirectory not setup")
	}
	return nil
}

func (it *Options) Reset() *Options {

	if it.WriteBufferSize == 0 {
		it.WriteBufferSize = 8
	} else if it.WriteBufferSize < 2 {
		it.WriteBufferSize = 2
	} else if it.WriteBufferSize > 256 {
		it.WriteBufferSize = 256
	}

	if it.BlockCacheSize == 0 {
		it.BlockCacheSize = 8
	} else if it.BlockCacheSize < 2 {
		it.BlockCacheSize = 2
	} else if it.BlockCacheSize > 1024 {
		it.BlockCacheSize = 1024
	}

	if it.MaxTableSize == 0 {
		it.MaxTableSize = 8
	} else if it.MaxTableSize < 2 {
		it.MaxTableSize = 2
	} else if it.MaxTableSize > 64 {
		it.MaxTableSize = 64
	}

	if it.MaxOpenFiles == 0 {
		it.MaxOpenFiles = 500
	} else if it.MaxOpenFiles < 100 {
		it.MaxOpenFiles = 100
	} else if it.MaxOpenFiles > 10000 {
		it.MaxOpenFiles = 10000
	}

	if it.Compression != "none" {
		it.Compression = "snappy"
	}

	return it
}
