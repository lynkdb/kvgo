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

package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	hauth "github.com/hooto/hauth/go/hauth/v1"
	"github.com/hooto/hlog4g/hlog"
	"github.com/hooto/hmetrics"
	"github.com/hooto/htoml4g/htoml"
	ps_disk "github.com/shirou/gopsutil/v3/disk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/lynkdb/kvgo/internal/utils"
	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

const (
	AppName = "kvgo"
)

var (
	version = "2.0.0"
	release = "dev.0"

	Prefix = ""

	serverMut sync.Mutex

	err error
)

type dbServer struct {
	mu  sync.Mutex
	pid uint64

	cfg     Config
	cfgFile string

	grpcListener net.Listener
	apiAdmin     *serviceAdminImpl
	api          *serviceApiImpl

	dbSystem *tableReplica

	volumes map[string]*ConfigVolume
	stores  map[string]storage.Conn

	tableSetupMux sync.Mutex

	tableMapMgr *tableMapMgr

	keyMgr *hauth.AccessKeyManager

	sysStatus sysStatus

	once sync.Once

	uptime int64
	close  bool

	closegw sync.WaitGroup
}

func Setup(ver, rel string) (*dbServer, error) {

	version = ver
	release = rel

	if Prefix, err = filepath.Abs(filepath.Dir(os.Args[0]) + "/.."); err != nil {
		Prefix = "/opt/lynkdb/" + AppName
	}

	var (
		cfgFile = Prefix + "/etc/kvgo-server.toml"
		cfg     Config
	)

	err := htoml.DecodeFromFile(cfgFile, &cfg)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return dbServerSetup(cfgFile, cfg)
}

func dbServerSetup(cfgFile string, cfg Config) (*dbServer, error) {

	serverMut.Lock()
	defer serverMut.Unlock()

	if cfg.Server.Bind == "" {
		cfg.Server.Bind = "127.0.0.1:9566"
	}

	if cfg.Storage.DataDirectory == "" {
		cfg.Storage.DataDirectory = Prefix + "/var/data"
	}

	cfg.Reset()

	srv := &dbServer{
		pid:         randUint64(),
		cfg:         cfg,
		cfgFile:     cfgFile,
		keyMgr:      hauth.NewAccessKeyManager(),
		volumes:     map[string]*ConfigVolume{},
		stores:      map[string]storage.Conn{},
		tableMapMgr: newTableMapMgr(&cfg),
	}

	{
		if err := srv.dbSystemSetup(); err != nil {
			return nil, err
		}
	}

	{
		if err := srv.dbVolumesSetup(); err != nil {
			hlog.Printf("error", "kvgo volumes setup error %s", err.Error())
			return nil, err
		}
	}

	{
		if err := srv.jobSetup(); err != nil {
			hlog.Printf("error", "kvgo job setup error %s", err.Error())
			return nil, err
		}
	}

	{
		for _, vol := range srv.cfg.Storage.Volumes {
			st, err := ps_disk.Usage(vol.Mountpoint)
			if err != nil {
				continue
			}
			used := int64(st.Used) / (1 << 20)
			srv.sysStatus.syncVolumeStatus(vol.Id, used, int64(st.Total/(1<<20))-used)
		}
	}

	{
		if srv.cfg.Server.HttpPort > 0 &&
			(srv.cfg.Server.PprofEnable || srv.cfg.Server.MetricsEnable) {

			if srv.cfg.Server.MetricsEnable {
				http.HandleFunc("/metrics", hmetrics.HttpHandler)
			}

			ln, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.cfg.Server.HttpPort))
			if err != nil {
				return nil, err
			}
			go http.Serve(ln, nil)

			hlog.Printf("info", "%s http listen :%d ok",
				AppName, srv.cfg.Server.HttpPort)
		}
	}

	{
		if err := srv.keyMgrSetup(); err != nil {
			return nil, err
		}

		if err := srv.netSetup(); err != nil {
			hlog.Printf("error", "kvgo net setup error %s", err.Error())
			return nil, err
		}
	}

	err := srv.ConfigFlush()
	if err != nil {
		return nil, err
	}

	srv.uptime = time.Now().Unix()

	go srv.once.Do(srv.jobOnce)

	return srv, nil
}

func (it *dbServer) dbSystemSetup() error {

	var (
		dir  = filepath.Clean(fmt.Sprintf("%s/%s_%s", it.cfg.Storage.DataDirectory, sysTableStoreId, storage.DriverV2))
		opts = &storage.Options{
			WriteBufferSize: 2,
			BlockCacheSize:  2,
			MaxTableSize:    8,
			MaxOpenFiles:    20,
			Compression:     "snappy",
		}
	)

	testPrintf("server %d, system dir %s", it.pid, dir)

	store, err := storage.Open(storage.DriverV2, dir, opts)
	if err != nil {
		return err
	}

	tdb, err := NewTable(store, sysTableId, sysTableName, 1, 2, &it.cfg)
	if err != nil {
		return err
	}
	it.dbSystem = tdb

	tm := it.tableMapMgr.syncTable(&kvapi.Meta{}, &kvapi.Table{
		Id:         sysTableId,
		Name:       sysTableName,
		ReplicaNum: 1,
	})

	tm.syncMap(&kvapi.Meta{}, &kvapi.TableMap{
		Id: sysTableId,
		Shards: []*kvapi.TableMap_Shard{
			{
				Id: 1,
				Replicas: []*kvapi.TableMap_Replica{
					{
						Id:      2,
						StoreId: sysTableStoreId,
					},
				},
			},
		},
	})

	tm.syncStore(sysTableStoreId, store)

	it.stores[sysTableStoreId] = store

	return nil
}

func (it *dbServer) ConfigFlush() error {
	return htoml.EncodeToFile(it.cfg, it.cfgFile, nil)
}

func (it *dbServer) keyMgrSetup() error {

	if it.cfg.Server.AccessKey == nil {
		return errors.New("no [server.access_key] setup")
	}

	it.keyMgr.KeySet(it.cfg.Server.AccessKey)

	for _, role := range defaultRoles {
		it.keyMgr.RoleSet(role)
	}

	return nil
}

func (it *dbServer) netSetup() error {
	host, port, err := net.SplitHostPort(it.cfg.Server.Bind)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	hlog.Printf("info", "server bind %s:%s", host, port)

	it.cfg.Server.Bind = host + ":" + port

	serverOptions := []grpc.ServerOption{
		grpc.MaxMsgSize(grpcMsgByteMax * 2),
		grpc.MaxSendMsgSize(grpcMsgByteMax * 2),
		grpc.MaxRecvMsgSize(grpcMsgByteMax * 2),
	}

	if it.cfg.Server.AuthTLSCert != nil {

		cert, err := tls.X509KeyPair(
			[]byte(it.cfg.Server.AuthTLSCert.ServerCertData),
			[]byte(it.cfg.Server.AuthTLSCert.ServerKeyData))
		if err != nil {
			return err
		}

		certs := credentials.NewServerTLSFromCert(&cert)

		serverOptions = append(serverOptions, grpc.Creds(certs))
	}

	grpcServer := grpc.NewServer(serverOptions...)

	it.apiAdmin = &serviceAdminImpl{
		rpcServer: grpcServer,
		dbServer:  it,
	}

	it.api = &serviceApiImpl{
		rpcServer: grpcServer,
		dbServer:  it,
	}

	kvapi.RegisterKvgoAdminServer(grpcServer, it.apiAdmin)
	kvapi.RegisterKvgoServer(grpcServer, it.api)

	go grpcServer.Serve(lis)

	it.grpcListener = lis

	return nil
}

func (it *dbServer) dbVolumesSetup() error {

	vols := []*ConfigVolume{}

	for _, vol := range it.cfg.Storage.Volumes {
		if vol == nil {
			continue
		}
		vol.Mountpoint = filepath.Clean(vol.Mountpoint)

		var item ConfigVolumeSetupMeta
		err := utils.JsonDecodeFromFile(vol.Mountpoint+"/.kvgo.volume.json", &item)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			item.Id = utils.RandUint64HexString()
			item.Engine = storage.DefaultDriver
			item.Created = uint64(time.Now().Unix())
		}
		item.Updated = uint64(time.Now().Unix())
		item.LoadCycleCount += 1

		if err = utils.JsonEncodeToFile(vol.Mountpoint+"/.kvgo.volume.json", &item); err != nil {
			return err
		}

		if vol.Id == "" {
			vol.Id = item.Id
		}

		if vol.Engine == "" {
			vol.Engine = item.Engine
		}

		if item.Id != vol.Id {
			return fmt.Errorf("volume (%s) setup fail : id conflict", vol.Id)
		}

		vols = append(vols, vol)
		it.volumes[vol.Id] = vol
	}

	if len(vols) > 0 {
		// jsonPrint("vols", vols)
	}

	it.cfg.Storage.Volumes = vols

	return nil
}

func (it *dbServer) Close() error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.close {
		it.close = true
		time.Sleep(1e9)

		it.closegw.Wait()

		it.grpcListener.Close()

		it.tableMapMgr.iter(func(tbl *tableMap) {
			tbl.Close()
		})

		/**
		for i, store := range it.stores {
			store.Close()
			jsonPrint("server try close store", i)
		}
		*/
	}

	return nil
}
