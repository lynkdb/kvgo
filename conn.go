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
	"os"

	"code.hooto.com/lynkdb/iomix/connect"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Conn struct {
	db   *leveldb.DB
	opts *options
}

type options struct {
	DataDir                string `json:"datadir,omitempty"`
	WriteBuffer            int    `json:"write_buffer,omitempty"`
	BlockCacheCapacity     int    `json:"block_cache_capacity,omitempty"`
	CacheCapacity          int    `json:"cache_capacity,omitempty"`
	OpenFilesCacheCapacity int    `json:"open_files_cache_capacity,omitempty"`
	CompactionTableSize    int    `json:"compaction_table_size,omitempty"`
}

func (opts *options) fix() {

	if opts.WriteBuffer < 4 {
		opts.WriteBuffer = 4
	} else if opts.WriteBuffer > 128 {
		opts.WriteBuffer = 128
	}

	if opts.CacheCapacity < 8 {
		opts.CacheCapacity = 8
	} else if opts.CacheCapacity > 4096 {
		opts.CacheCapacity = 4096
	}

	if opts.BlockCacheCapacity < 2 {
		opts.BlockCacheCapacity = 2
	} else if opts.BlockCacheCapacity > 32 {
		opts.BlockCacheCapacity = 32
	}

	if opts.OpenFilesCacheCapacity < 500 {
		opts.OpenFilesCacheCapacity = 500
	} else if opts.OpenFilesCacheCapacity > 30000 {
		opts.OpenFilesCacheCapacity = 30000
	}

	if opts.CompactionTableSize < 2 {
		opts.CompactionTableSize = 2
	} else if opts.CompactionTableSize > 128 {
		opts.CompactionTableSize = 128
	}
}

func Open(copts connect.ConnOptions) (*Conn, error) {

	var (
		cn = &Conn{
			opts: &options{},
		}
		err error
	)

	if v, ok := copts.Items.Get("data_dir"); ok {
		cn.opts.DataDir = v.String()
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/write_buffer"); ok {
		cn.opts.WriteBuffer = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/block_cache_capacity"); ok {
		cn.opts.BlockCacheCapacity = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/open_files_cache_capacity"); ok {
		cn.opts.OpenFilesCacheCapacity = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/compaction_table_size"); ok {
		cn.opts.CompactionTableSize = v.Int()
	}

	cn.opts.fix()

	if err := os.MkdirAll(cn.opts.DataDir, 0750); err != nil {
		return cn, err
	}

	cn.db, err = leveldb.OpenFile(cn.opts.DataDir, &opt.Options{
		WriteBuffer:            cn.opts.WriteBuffer * opt.MiB,
		BlockCacheCapacity:     cn.opts.BlockCacheCapacity * opt.MiB,
		OpenFilesCacheCapacity: cn.opts.OpenFilesCacheCapacity,
		CompactionTableSize:    cn.opts.CompactionTableSize * opt.MiB,
		Compression:            opt.SnappyCompression,
		Filter:                 filter.NewBloomFilter(10),
	})

	if err == nil {
		cn.ttl_worker()
	}

	return cn, err
}

func (cn *Conn) Close() error {

	if cn.db != nil {
		return cn.db.Close()
	}

	return nil
}
