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
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/sko"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	skoConnMu sync.Mutex
	skoConns  = map[string]*SkoConn{}
)

type SkoConn struct {
	db            *leveldb.DB
	opts          *options
	clients       int
	skoMu         sync.RWMutex
	skoLogMu      sync.RWMutex
	skoLogOffset  uint64
	skoLogCutset  uint64
	skoIncrMu     sync.RWMutex
	skoIncrOffset uint64
	skoIncrCutset uint64
	skoCluster    *SkoServiceImpl
}

func SkoOpen(copts connect.ConnOptions) (*SkoConn, error) {

	skoConnMu.Lock()
	defer skoConnMu.Unlock()

	var (
		cn = &SkoConn{
			opts:          &options{},
			clients:       1,
			skoLogOffset:  0,
			skoLogCutset:  0,
			skoIncrOffset: 0,
			skoIncrCutset: 0,
		}
		err error
	)

	if v, ok := copts.Items.Get("data_dir"); !ok {
		return nil, errors.New("No data_dir Found")
	} else {
		cn.opts.DataDir = filepath.Clean(v.String())
	}

	if pconn, ok := skoConns[cn.opts.DataDir]; ok {
		pconn.clients++
		return pconn, nil
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/write_buffer"); ok {
		cn.opts.WriteBuffer = v.Int()
	}

	if v, ok := copts.Items.Get("lynkdb/sskv/cache_capacity"); ok {
		cn.opts.CacheCapacity = v.Int()
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

	if v, ok := copts.Items.Get("lynkdb/sko/cluster_bind"); ok {
		cn.opts.ClusterBind = v.String()
	}

	if v, ok := copts.Items.Get("lynkdb/sko/cluster_nodes"); ok {
		cn.opts.ClusterNodes = strings.Split(v.String(), ",")
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

	if nCap := len(cn.opts.ClusterNodes); nCap > 0 {

		if nCap > sko.ObjectClusterNodeMax {
			return nil, errors.New("Deny of sko.ObjectClusterNodeMax")
		}

		hosts := map[string]bool{}

		for _, v := range cn.opts.ClusterNodes {
			_, _, err := net.SplitHostPort(v)
			if err != nil {
				return nil, err
			}
			if _, ok := hosts[v]; ok {
				return nil, errors.New("Duplicate host:port " + v)
			}
			hosts[v] = true
		}

		if err := cn.ClusterStart(); err != nil {
			cn.Close()
			return nil, err
		}
	}

	go cn.skoWorker()

	skoConns[cn.opts.DataDir] = cn

	return cn, nil
}

func (cn *SkoConn) Close() error {

	skoConnMu.Lock()
	defer skoConnMu.Unlock()

	if pconn, ok := skoConns[cn.opts.DataDir]; ok {

		if pconn.clients > 1 {
			pconn.clients--
			return nil
		}
	}

	if cn.skoCluster != nil && cn.skoCluster.sock != nil {
		cn.skoCluster.sock.Close()
	}

	if cn.db != nil {
		cn.db.Close()
	}

	delete(skoConns, cn.opts.DataDir)

	return nil
}
