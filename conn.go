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
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hooto/hauth/go/hauth/v1"
	"github.com/hooto/hflag4g/hflag"
	"github.com/hooto/hlog4g/hlog"
	tsd2 "github.com/valuedig/apis/go/tsd/v2"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

var (
	connMu sync.Mutex
	conns  = map[string]*Conn{}
)

type Conn struct {
	mu                    sync.RWMutex
	dbmu                  sync.Mutex
	dbSys                 kv2.StorageEngine
	tables                map[string]*dbTable
	opts                  *Config
	clients               int
	client                *kv2.PublicClient
	grpcListener          net.Listener
	public                *PublicServiceImpl
	internal              *InternalServiceImpl
	keyMgr                *hauth.AccessKeyManager
	close                 bool
	workmu                sync.Mutex
	workerLocalRunning    bool
	uptime                int64
	workerTableRefreshed  int64
	workerStatusRefreshed int64
	sysStatus             *kv2.SysNodeStatus
	monitor               *tsd2.SampleSet
	taskLocks             sync.Map
}

func Open(args ...interface{}) (*Conn, error) {

	if len(args) < 1 {
		return nil, errors.New("no config setup")
	}

	connMu.Lock()
	defer connMu.Unlock()

	var (
		cn = &Conn{
			clients:   1,
			keyMgr:    hauth.NewAccessKeyManager(),
			tables:    map[string]*dbTable{},
			opts:      &Config{},
			uptime:    time.Now().Unix(),
			sysStatus: &kv2.SysNodeStatus{},
		}
	)

	for _, cfg := range args {

		switch cfg.(type) {

		case Config:
			c := cfg.(Config)
			cn.opts = &c

		case *Config:
			cn.opts = cfg.(*Config)

		case ConfigStorage:
			c := cfg.(ConfigStorage)
			cn.opts.Storage = c

		case ConfigServer:
			cn.opts.Server = cfg.(ConfigServer)

		case ConfigPerformance:
			cn.opts.Performance = cfg.(ConfigPerformance)

		case ConfigFeature:
			cn.opts.Feature = cfg.(ConfigFeature)

		case ConfigCluster:
			cn.opts.Cluster = cfg.(ConfigCluster)

		default:
			return nil, errors.New("invalid config")
		}
	}

	cn.opts.Reset()

	if err := cn.opts.Valid(); err != nil {
		return nil, err
	}

	if cn.opts.Storage.DataDirectory == "" {
		cn.opts.ClientConnectEnable = true
	}

	if cn.opts.ClientConnectEnable {

		if err := cn.serviceStart(); err != nil {
			cn.closeForce()
			return nil, err
		}
		hlog.Printf("info", "kvgo client connected")
		return cn, nil
	}

	if pconn, ok := conns[cn.opts.Storage.DataDirectory]; ok {
		pconn.clients++
		return pconn, nil
	}

	if cn.opts.Storage.DataDirectory != "" {

		if err := cn.dbSysSetup(); err != nil {
			hlog.Printf("error", "kvgo db-meta setup error %s", err.Error())
			return nil, err
		}

		if err := cn.dbTableListSetup(); err != nil {
			hlog.Printf("error", "kvgo db-table setup error %s", err.Error())
			return nil, err
		}
	}

	cn.monitorSetup()

	if err := cn.serviceStart(); err != nil {
		cn.closeForce()
		return nil, err
	}

	cn.sysStatus.Id = cn.opts.Server.ID

	go cn.workerLocal()

	hlog.Printf("info", "kvgo started (%s)", cn.opts.Storage.DataDirectory)

	conns[cn.opts.Storage.DataDirectory] = cn

	time.Sleep(500e6)

	return cn, nil
}

func (it *Conn) NewClient() (kv2.Client, error) {
	return kv2.NewClient(it)
}

func (cn *Conn) tabledb(name string) *dbTable {
	if name == "" {
		name = "main"
	}
	dt := cn.tables[name]
	if dt != nil && dt.db != nil {
		return dt
	}
	return nil
}

func (cn *Conn) dbSetup(dir string, opts *kv2.StorageOptions) (*dbTable, error) {

	if cn.opts.Storage.Engine == "leveldb_to_pebble" || cn.opts.Storage.Engine == "pebble" {

		if _, err := os.Stat(dir + "_pebble"); err != nil {

			dbDst, err := StoragePebbleOpen(dir+"_pebble", opts)
			if err != nil {
				return nil, err
			}

			dbSrc, err := StorageLevelDBOpen(dir, opts)
			if err != nil {
				return nil, err
			}

			siz := []*kv2.StorageIteratorRange{
				{
					Start: []byte{},
					Limit: []byte{0xff, 0xff},
				},
			}

			var (
				t1   = time.Now()
				num  = 0
				iter = dbSrc.NewIterator(siz[0])
			)

			for ok := iter.First(); ok; ok = iter.Next() {
				if ss := dbDst.Put(iter.Key(), iter.Value(), nil); ss.OK() {
					num += 1
				} else {
					hlog.Printf("info", "db upgrading %d, err %v", num, ss.Error())
				}
				if (num % 1e4) == 0 {
					hlog.Printf("info", "db upgrading %d, time %v", num, time.Since(t1))
				}
			}

			hlog.Printf("info", "db upgrading %d, time %v, dir %s, done", num, time.Since(t1), dir)

			iter.Release()

			dbSrc.Close()
			dbDst.Close()
		}

		if _, err := os.Stat(dir); err == nil {
			if err = os.Rename(dir, fmt.Sprintf("%s_leveldb_%s", dir, time.Now().Format("20060102_150405"))); err != nil {
				return nil, err
			}
		}

		cn.opts.Storage.Engine = "pebble"
	}

	if cn.opts.Storage.Engine == "pebble" && !strings.HasSuffix(dir, "_pebble") {
		dir += "_pebble"
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	db, err := storageEngineOpen(cn.opts.Storage.Engine, dir, opts)
	if err != nil {
		return nil, err
	}

	if _, ok := hflag.ValueOK("db-ns-stats"); ok {

		for _, v := range []uint8{
			nsKeySys,
			nsKeyMeta,
			nsKeyData,
			nsKeyLog,
			nsKeyTtl,
		} {

			if strings.HasSuffix(dir, "/sys") ||
				strings.HasSuffix(dir, "/sys_pebble") {
				continue
			}

			iter := db.NewIterator(&kv2.StorageIteratorRange{
				Start: []byte{v},
				Limit: []byte{v, 0xff},
			})
			defer iter.Release()

			num := 0

			for ok := iter.First(); ok; ok = iter.Next() {
				num++
			}

			if num == 0 {
				continue
			}

			hlog.Printf("info", "db-ns-stats table %s, ns %d, num %d",
				dir, v, num)

			if v == nsKeyLog {

				if iter.Prev() {
					meta, _, err := kv2.ObjectMetaDecode(bytesClone(iter.Value()))
					if err == nil {
						hlog.Printf("info", "db-ns-stats table %s, ns %d, log-id %d",
							dir, v, meta.Version)
					}
				}
			}
		}
	}

	dt := &dbTable{
		db:             db,
		incrSets:       map[string]*dbTableIncrSet{},
		logPullPending: map[string]bool{},
		logPullOffsets: map[string]*dbTableLogPullOffset{},
		logLockSets:    map[uint64]uint64{},
		monitor:        cn.monitor,
		logSyncBuffer:  newLogSyncBufferTable(),
	}

	rs := dt.db.Get(keySysInstanceId, nil)
	if rs.Error() == nil {
		dt.instId = rs.String()
	} else if rs.NotFound() {
		dt.instId = randHexString(16)
		rs = dt.db.Put(keySysInstanceId, []byte(dt.instId), nil)
	}

	if rs.Error() != nil {
		dt.Close()
		dt = nil
	}

	return dt, err
}

func (cn *Conn) dbSysSetup() error {

	var (
		dir  = filepath.Clean(fmt.Sprintf("%s/%s", cn.opts.Storage.DataDirectory, sysTableName))
		opts = &kv2.StorageOptions{
			WriteBufferSize:   2,
			BlockCacheSize:    2,
			MaxTableSize:      2,
			MaxOpenFiles:      10,
			TableCompressName: "none",
		}
	)

	dt, err := cn.dbSetup(dir, opts)
	if err != nil {
		return err
	}

	cn.dbSys = dt.db
	cn.tables[sysTableName] = &dbTable{
		tableId:        0,
		tableName:      sysTableName,
		db:             dt.db,
		incrSets:       map[string]*dbTableIncrSet{},
		logPullPending: map[string]bool{},
		logPullOffsets: map[string]*dbTableLogPullOffset{},
		logLockSets:    map[uint64]uint64{},
		monitor:        cn.monitor,
		logSyncBuffer:  newLogSyncBufferTable(),
	}

	if cn.opts.Server.Bind != "" {

		rr2 := kv2.NewObjectReader(nil).
			TableNameSet(sysTableName).
			KeyRangeSet(nsSysAccessKey(""), append(nsSysAccessKey(""), 0xff)).
			LimitNumSet(1000)

		if rs := cn.objectLocalQuery(rr2); rs.OK() {
			for _, v := range rs.Items {
				var key hauth.AccessKey
				if err := v.DataValue().Decode(&key, nil); err == nil {
					cn.keyMgr.KeySet(&key)
				}
			}
			hlog.Printf("info", "server load access keys %d", len(rs.Items))
		}

		{
			if !hex16.MatchString(cn.opts.Server.ID) {
				if rs2 := cn.objectLocalQuery(kv2.NewObjectReader(nsSysServerId()).
					TableNameSet(sysTableName)); rs2.OK() {
					cn.opts.Server.ID = rs2.DataValue().String()
				}

				if !hex16.MatchString(cn.opts.Server.ID) {
					cn.opts.Server.ID = randHexString(16)
				}
			}

			if rs2 := cn.commitLocal(kv2.NewObjectWriter(nsSysServerId(), []byte(cn.opts.Server.ID)).
				TableNameSet(sysTableName), 0); !rs2.OK() {
				return rs2.Error()
			}

			hlog.Printf("info", "server id %s", cn.opts.Server.ID)
		}

		if cn.opts.Server.AccessKey != nil &&
			len(cn.opts.Server.AccessKey.Secret) > 20 {
			key := cn.opts.Server.AccessKey
			if pkey := cn.keyMgr.KeyGet(key.Id); pkey == nil || key.Secret != pkey.Secret {

				rootKey := NewSystemAccessKey()
				key.Roles = rootKey.Roles
				key.Scopes = rootKey.Scopes

				rr2 := kv2.NewObjectWriter(nsSysAccessKey(key.Id), key).
					TableNameSet(sysTableName)
				tdb := cn.tabledb(sysTableName)
				if tdb != nil {
					cn.commitLocal(rr2, 0)
					cn.keyMgr.KeySet(key)
					hlog.Printf("warn", "server force rewrite root access key")
				}
			}
		}

		for _, role := range defaultRoles {
			cn.keyMgr.RoleSet(role)
		}
	}

	return nil
}

func (cn *Conn) dbTableListSetup() error {

	tables := map[string]*dbTable{
		"main": {
			tableId:        10,
			tableName:      "main",
			incrSets:       map[string]*dbTableIncrSet{},
			logPullPending: map[string]bool{},
			logPullOffsets: map[string]*dbTableLogPullOffset{},
			logLockSets:    map[uint64]uint64{},
			logSyncBuffer:  newLogSyncBufferTable(),
		},
	}

	for _, t := range tables {

		k := nsSysTable(t.tableName)

		if rs := cn.dbSys.Get(keyEncode(nsKeyData, k), nil); !rs.OK() {

			if rs.NotFound() {

				obj := kv2.NewObjectWriter(k, &kv2.TableItem{
					Name: t.tableName,
				}).IncrNamespaceSet(sysTableIncrNS)

				obj.Meta.IncrId = uint64(t.tableId)
				obj.TableName = sysTableName

				rs := cn.commitLocal(obj, 0)
				if !rs.OK() {
					return errors.New(rs.Message)
				}

				hlog.Printf("info", "init db %s table ok", sysTableName)

			} else {
				return rs.Error()
			}
		}
	}

	var (
		offset = keyEncode(nsKeyData, nsSysTable(""))
		cutset = keyEncode(nsKeyData, nsSysTable(""))
		values = [][]byte{}
	)
	cutset = append(cutset, 0xff)

	iter := cn.dbSys.NewIterator(&kv2.StorageIteratorRange{
		Start: offset,
		Limit: cutset,
	})
	defer iter.Release()

	for ok := iter.First(); ok; ok = iter.Next() {

		if bytes.Compare(iter.Key(), offset) <= 0 {
			continue
		}

		if bytes.Compare(iter.Key(), cutset) >= 0 {
			break
		}

		values = append(values, bytesClone(iter.Value()))
	}

	if iter.Error() != nil {
		return iter.Error()
	}

	for _, bs := range values {

		item, err := kv2.ObjectItemDecode(bs)
		if err != nil {
			return err
		}

		var tb kv2.TableItem
		if err = item.DataValue().Decode(&tb, nil); err != nil {
			return err
		}

		if tables[tb.Name] != nil &&
			uint64(tables[tb.Name].tableId) != item.Meta.IncrId {
			return fmt.Errorf("table name (%s) conflict", tb.Name)
		}

		if tb.Name == sysTableName {
			continue
		}

		tables[tb.Name] = &dbTable{
			tableId:        uint32(item.Meta.IncrId),
			tableName:      tb.Name,
			incrSets:       map[string]*dbTableIncrSet{},
			logPullPending: map[string]bool{},
			logPullOffsets: map[string]*dbTableLogPullOffset{},
			logLockSets:    map[uint64]uint64{},
			monitor:        cn.monitor,
			logSyncBuffer:  newLogSyncBufferTable(),
		}
	}

	for _, t := range tables {

		if err := cn.dbTableSetup(t.tableName, t.tableId); err != nil {
			return err
		}

		hlog.Printf("info", "kvgo table %s (%d) started", t.tableName, t.tableId)
	}

	return nil
}

func (cn *Conn) dbTableSetup(tableName string, tableId uint32) error {

	cn.dbmu.Lock()
	defer cn.dbmu.Unlock()

	tdb := cn.tabledb(tableName)
	if tdb != nil {
		return nil
	}

	dir := filepath.Clean(fmt.Sprintf("%s/%d_%d_%d", cn.opts.Storage.DataDirectory,
		tableId, 0, 0))

	opts := &kv2.StorageOptions{
		WriteBufferSize:   cn.opts.Performance.WriteBufferSize,
		BlockCacheSize:    cn.opts.Performance.BlockCacheSize,
		MaxTableSize:      cn.opts.Performance.MaxTableSize,
		MaxOpenFiles:      cn.opts.Performance.MaxOpenFiles,
		TableCompressName: cn.opts.Feature.TableCompressName,
	}

	dt, err := cn.dbSetup(dir, opts)
	if err != nil {
		return err
	}

	dt.tableId, dt.tableName = tableId, tableName

	if err = dt.setup(); err != nil {
		return err
	}

	hlog.Printf("info", "setup table %s ok", tableName)
	cn.tables[tableName] = dt

	return nil
}

func (cn *Conn) OptionApply(opts ...kv2.ClientOption) {
	// TODO
}

func (cn *Conn) Close() error {

	connMu.Lock()
	defer connMu.Unlock()

	cn.close = true

	if cn.grpcListener != nil {
		cn.grpcListener.Close()
	}

	time.Sleep(500e6)

	return cn.closeForce()
}

func (cn *Conn) closeForce() error {

	if pconn, ok := conns[cn.opts.Storage.DataDirectory]; ok {

		if pconn.clients > 1 {
			pconn.clients--
			return nil
		}
	}

	if cn.public != nil && cn.public.sock != nil {
		cn.public.sock.Close()
	}

	if cn.monitor != nil {
		cn.monitor.Flush(true)
		cn.monitor.Close()
	}

	for _, tdb := range cn.tables {
		tdb.Close()
	}

	if cn.dbSys != nil {
		// cn.dbSys.Close()
	}

	delete(conns, cn.opts.Storage.DataDirectory)

	return nil
}

func (it *Conn) OpenTable(tableName string) kv2.ClientTable {
	return kv2.NewClientTable(it, tableName)
}
