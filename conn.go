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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hooto/hlog4g/hlog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/lynkdb/iomix/connect"
)

var (
	connMu sync.Mutex
	conns  = map[string]*Conn{}
)

type options struct {
	DataDir                string   `json:"datadir,omitempty"`
	WriteBuffer            int      `json:"write_buffer,omitempty"`
	BlockCacheCapacity     int      `json:"block_cache_capacity,omitempty"`
	CacheCapacity          int      `json:"cache_capacity,omitempty"`
	OpenFilesCacheCapacity int      `json:"open_files_cache_capacity,omitempty"`
	CompactionTableSize    int      `json:"compaction_table_size,omitempty"`
	ClusterBind            string   `json:"cluster_bind,omitempty"`
	ClusterMasters         []string `json:"cluster_masters,omitempty"`
	ClusterSecretKey       string   `json:"cluster_secret_key,omitempty"`
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

type Conn struct {
	instId     string
	db         *leveldb.DB
	opts       *options
	clients    int
	mu         sync.RWMutex
	logMu      sync.RWMutex
	logOffset  uint64
	logCutset  uint64
	incrMu     sync.RWMutex
	incrOffset uint64
	incrCutset uint64
	cluster    *ServiceImpl
}

func Open(copts connect.ConnOptions) (*Conn, error) {

	connMu.Lock()
	defer connMu.Unlock()

	var (
		cn = &Conn{
			opts:       &options{},
			clients:    1,
			logOffset:  0,
			logCutset:  0,
			incrOffset: 0,
			incrCutset: 0,
		}
		err error
	)

	if v, ok := copts.Items.Get("data_dir"); !ok {
		return nil, errors.New("No data_dir Found")
	} else {
		cn.opts.DataDir = filepath.Clean(v.String())
	}

	if pconn, ok := conns[cn.opts.DataDir]; ok {
		pconn.clients++
		return pconn, nil
	}

	if v, ok := copts.Items.Get("lynkdb/sko/write_buffer"); ok {
		cn.opts.WriteBuffer = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/cache_capacity"); ok {
		cn.opts.CacheCapacity = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/block_cache_capacity"); ok {
		cn.opts.BlockCacheCapacity = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/open_files_cache_capacity"); ok {
		cn.opts.OpenFilesCacheCapacity = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/compaction_table_size"); ok {
		cn.opts.CompactionTableSize = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/cluster_bind"); ok {
		cn.opts.ClusterBind = v.String()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/cluster_masters"); ok {
		cn.opts.ClusterMasters = strings.Split(v.String(), ",")
	}

	if v, ok := copts.Items.Get("lynkdb/sko/cluster_secret_key"); ok {
		cn.opts.ClusterSecretKey = v.String()
	}

	cn.opts.fix()

	cn.opts.DataDir = filepath.Clean(fmt.Sprintf("%s/%d_%d_%d", cn.opts.DataDir, 10, 0, 0))

	if err := os.MkdirAll(cn.opts.DataDir, 0750); err != nil {
		return cn, err
	}

	if cn.db, err = leveldb.OpenFile(cn.opts.DataDir, &opt.Options{
		WriteBuffer:            cn.opts.WriteBuffer * opt.MiB,
		BlockCacheCapacity:     cn.opts.BlockCacheCapacity * opt.MiB,
		OpenFilesCacheCapacity: cn.opts.OpenFilesCacheCapacity,
		CompactionTableSize:    cn.opts.CompactionTableSize * opt.MiB,
		Compression:            opt.SnappyCompression,
		Filter:                 filter.NewBloomFilter(10),
	}); err != nil {
		return nil, err
	}

	if bs, err := cn.db.Get(keySysInstanceId, nil); err == nil {
		cn.instId = string(bs)
	} else if err.Error() == ldbNotFound {
		cn.instId = randHexString(16)
		if err := cn.db.Put(keySysInstanceId, []byte(cn.instId), nil); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	if err := cn.clusterStart(); err != nil {
		cn.Close()
		return nil, err
	}

	go cn.workerLocal()

	hlog.Printf("info", "kvgo %s started", cn.instId)

	conns[cn.opts.DataDir] = cn

	return cn, nil
}

func (cn *Conn) Close() error {

	connMu.Lock()
	defer connMu.Unlock()

	if pconn, ok := conns[cn.opts.DataDir]; ok {

		if pconn.clients > 1 {
			pconn.clients--
			return nil
		}
	}

	if cn.cluster != nil && cn.cluster.sock != nil {
		cn.cluster.sock.Close()
	}

	if cn.db != nil {
		cn.db.Close()
	}

	delete(conns, cn.opts.DataDir)

	return nil
}
