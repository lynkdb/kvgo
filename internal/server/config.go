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
	"io/ioutil"
	"path/filepath"
	"strings"

	hauth "github.com/hooto/hauth/go/hauth/v1"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type Config struct {

	// Storage Settings
	Storage ConfigStorage `toml:"storage" json:"storage" desc:"Storage Settings"`

	// Server Settings
	Server ConfigServer `toml:"server" json:"server" desc:"Server Settings"`

	// Performance Settings
	Performance ConfigPerformance `toml:"performance" json:"performance" desc:"Performance Settings"`

	// Feature Settings
	Feature ConfigFeature `toml:"feature" json:"feature"`

	// Cluster Settings
	Cluster ConfigCluster `toml:"cluster" json:"cluster" desc:"Cluster Settings"`
}

type ConfigStorage struct {
	DataDirectory string `toml:"data_directory" json:"data_directory"`
	Engine        string `toml:"engine" json:"engine"`

	Stores []*ConfigStore `toml:"stores,omitempty" json:"stores,omitempty"`
}

type ConfigStore struct {
	UniId      string `toml:"uni_id" json:"uni_id"`
	Name       string `toml:"name,omitempty" json:"name,omitempty"`
	Engine     string `toml:"engine" json:"engine"`
	Mountpoint string `toml:"mountpoint" json:"mountpoint"`

	StoreId uint64 `toml:"store_id" json:"store_id"`
}

type ConfigStoreSetupMeta struct {
	UniId   string `json:"uni_id"`
	Engine  string `json:"engine"`
	Created uint64 `json:"created"`
	Updated uint64 `json:"updated"`

	LoadCycleCount uint64 `json:"load_cycle_count"`
}

type ConfigTLSCertificate struct {
	ServerKeyFile  string `toml:"server_key_file" json:"server_key_file"`
	ServerKeyData  string `toml:"server_key_data" json:"server_key_data"`
	ServerCertFile string `toml:"server_cert_file" json:"server_cert_file"`
	ServerCertData string `toml:"server_cert_data" json:"server_cert_data"`
}

type ConfigServer struct {
	ID          string                `toml:"id" json:"id"`
	Bind        string                `toml:"bind" json:"bind"`
	AccessKey   *hauth.AccessKey      `toml:"access_key" json:"access_key"`
	AuthTLSCert *ConfigTLSCertificate `toml:"auth_tls_cert" json:"auth_tls_cert"`

	HttpPort uint16 `toml:"http_port,omitempty" json:"http_port,omitempty"`

	RuntimeMode string `toml:"runtime_mode,omitempty" json:"runtime_mode,omitempty"`

	PprofEnable bool `toml:"pprof_enable,omitempty" json:"pprof_enable,omitempty"`

	MetricsEnable bool `toml:"metrics_enable,omitempty" json:"metrics_enable,omitempty"`
}

type ConfigPerformance struct {
	WriteBufferSize int `toml:"write_buffer_size" json:"write_buffer_size" desc:"in MiB, default to 8"`
	BlockCacheSize  int `toml:"block_cache_size" json:"block_cache_size" desc:"in MiB, default to 32"`
	MaxTableSize    int `toml:"max_table_size" json:"max_table_size" desc:"in MiB, default to 8"`
	MaxOpenFiles    int `toml:"max_open_files" json:"max_open_files" desc:"default to 500"`
}

type ConfigFeature struct {
	WriteMetaDisable bool   `toml:"write_meta_disable" json:"write_meta_disable"`
	WriteLogDisable  bool   `toml:"write_log_disable" json:"write_log_disable"`
	Compression      string `toml:"compression" json:"compression"`
}

type ConfigCluster struct {
	//
	// MainNodes []*ClientConfig `toml:"main_nodes" json:"main_nodes"`

	// Replica-Of nodes settings
	// ReplicaOfNodes []*ConfigReplicaOfNode `toml:"replica_of_nodes" json:"replica_of_nodes" desc:"Replica-Of nodes settings"`
}

// type ConfigReplicaOfNode struct {
// 	*ClientConfig
// 	DatabaseMaps []*ConfigReplicaDatabaseMap `toml:"table_maps" json:"table_maps"`
// }
//
// type ConfigReplicaDatabaseMap struct {
// 	From string `toml:"from" json:"from"`
// 	To   string `toml:"to" json:"to"`
// }
//
// func (it *ConfigCluster) Master(addr string) *ClientConfig {
//
// 	for _, v := range it.MainNodes {
// 		if addr == v.Addr {
// 			return v
// 		}
// 	}
// 	return nil
// }
//
// func (it *ConfigCluster) randMainNodes(cap int) []*ClientConfig {
//
// 	if len(it.MainNodes) == 0 {
// 		return nil
// 	}
//
// 	var (
// 		ls     = []*ClientConfig{}
// 		offset = rand.Intn(len(it.MainNodes))
// 	)
//
// 	for i := offset; i < len(it.MainNodes) && len(ls) <= cap; i++ {
// 		ls = append(ls, it.MainNodes[i])
// 	}
// 	for i := 0; i < offset && len(ls) <= cap; i++ {
// 		ls = append(ls, it.MainNodes[i])
// 	}
//
// 	return ls
// }

func (it *ConfigServer) IsStandaloneMode() bool {
	return it.runtimeModeAllow(StandaloneMode)
}

func (it *ConfigServer) runtimeModeAllow(s string) bool {
	return it.RuntimeMode == s
}

func (it *Config) Valid() error {

	// if it.ClientConnectEnable {
	// 	if len(it.Cluster.MainNodes) < 1 {
	// 		return errors.New("no cluster/main_nodes setup")
	// 	}
	// }

	return nil
}

func NewConfig(dir string) *Config {
	return &Config{
		Storage: ConfigStorage{
			DataDirectory: filepath.Clean(dir),
		},
	}
}

func (it *Config) Reset() *Config {

	if it.Storage.Engine == "" {
		it.Storage.Engine = storage.DriverV2
	}

	if it.Server.ID == "" {
		it.Server.ID = randHexString(16)
	}

	if it.Server.RuntimeMode == "" {
		it.Server.RuntimeMode = StandaloneMode
	}

	if it.Performance.WriteBufferSize < 2 {
		it.Performance.WriteBufferSize = 2
	} else if it.Performance.WriteBufferSize > 256 {
		it.Performance.WriteBufferSize = 256
	}

	if it.Performance.BlockCacheSize < 4 {
		it.Performance.BlockCacheSize = 4
	} else if it.Performance.BlockCacheSize > 1024 {
		it.Performance.BlockCacheSize = 1024
	}

	if it.Performance.MaxTableSize < 2 {
		it.Performance.MaxTableSize = 2
	} else if it.Performance.MaxTableSize > 64 {
		it.Performance.MaxTableSize = 64
	}

	if it.Performance.MaxOpenFiles < 500 {
		it.Performance.MaxOpenFiles = 500
	} else if it.Performance.MaxOpenFiles > 10000 {
		it.Performance.MaxOpenFiles = 10000
	}

	if it.Feature.Compression != "none" {
		it.Feature.Compression = "snappy"
	}

	if it.Server.Bind != "" {
		if it.Server.AccessKey == nil {
			it.Server.AccessKey = NewSystemAccessKey()
		}
	}

	if it.Server.AuthTLSCert != nil {

		if it.Server.AuthTLSCert.ServerKeyFile != "" &&
			it.Server.AuthTLSCert.ServerKeyData == "" {
			if bs, err := ioutil.ReadFile(it.Server.AuthTLSCert.ServerKeyFile); err == nil {
				it.Server.AuthTLSCert.ServerKeyData = strings.TrimSpace(string(bs))
			}
		}

		if it.Server.AuthTLSCert.ServerCertFile != "" &&
			it.Server.AuthTLSCert.ServerCertData == "" {
			if bs, err := ioutil.ReadFile(it.Server.AuthTLSCert.ServerCertFile); err == nil {
				it.Server.AuthTLSCert.ServerCertData = strings.TrimSpace(string(bs))
			}
		}
	}

	return it
}

func (it *Config) cloneStorageOptions() *storage.Options {
	it.Reset()
	opts := &storage.Options{
		WriteBufferSize: uint64(it.Performance.WriteBufferSize),
		BlockCacheSize:  it.Performance.BlockCacheSize,
		MaxTableSize:    64,
		MaxOpenFiles:    it.Performance.MaxOpenFiles,
		Compression:     it.Feature.Compression,
	}
	return opts
}
