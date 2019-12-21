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
	"path/filepath"
	"strings"

	"github.com/lynkdb/iomix/connect"
)

type Config struct {

	// Storage Settings
	StorageDataDirectory string `json:"storage_data_directory"`

	// Server Settings
	ServerBind          string `json:"server_bind,omitempty"`
	ServerAuthSecretKey string `json:"server_auth_secret_key"`

	// Performance Settings
	PerformanceWriteBufferSize int `json:"performance_write_buffer_size,omitempty"`
	PerformanceBlockCacheSize  int `json:"performance_block_cache_size,omitempty"`
	PerformanceMaxTableSize    int `json:"performance_max_table_size,omitempty"`
	PerformanceMaxOpenFiles    int `json:"performance_max_open_files,omitempty"`

	// Feature Settings
	FeatureWriteMetaDisable bool `json:"feature_write_meta_disable,omitempty"`
	FeatureWriteLogDisable  bool `json:"feature_write_log_disable,omitempty"`

	// Cluster Settings
	ClusterMasters       []string `json:"cluster_masters,omitempty"`
	ClusterAuthSecretKey string   `json:"cluster_auth_secret_key,omitempty"`
}

func NewConfig(dir string) *Config {
	return &Config{
		StorageDataDirectory: filepath.Clean(dir),
	}
}

func (it *Config) reset() *Config {

	if it.PerformanceWriteBufferSize < 4 {
		it.PerformanceWriteBufferSize = 4
	} else if it.PerformanceWriteBufferSize > 128 {
		it.PerformanceWriteBufferSize = 128
	}

	if it.PerformanceBlockCacheSize < 8 {
		it.PerformanceBlockCacheSize = 8
	} else if it.PerformanceBlockCacheSize > 4096 {
		it.PerformanceBlockCacheSize = 4096
	}

	if it.PerformanceMaxTableSize < 8 {
		it.PerformanceMaxTableSize = 8
	} else if it.PerformanceMaxTableSize > 64 {
		it.PerformanceMaxTableSize = 64
	}

	if it.PerformanceMaxOpenFiles < 500 {
		it.PerformanceMaxOpenFiles = 500
	} else if it.PerformanceMaxOpenFiles > 10000 {
		it.PerformanceMaxOpenFiles = 10000
	}

	return it
}

func configParse(opts connect.ConnOptions) (*Config, error) {

	cfg := &Config{}

	// Storage Settings
	{
		if v, ok := opts.Items.Get("storage/data_directory"); ok {
			cfg.StorageDataDirectory = filepath.Clean(v.String())
		} else if v, ok := opts.Items.Get("data_dir"); ok {
			cfg.StorageDataDirectory = filepath.Clean(v.String())
		} else {
			return nil, errors.New("No storage/data_directory Found")
		}
	}

	// Server Settings
	{
		if v, ok := opts.Items.Get("server/bind"); ok {
			cfg.ServerBind = v.String()
		}
	}

	// Performance Settings
	{
		if v, ok := opts.Items.Get("performance/write_buffer_size"); ok {
			cfg.PerformanceWriteBufferSize = v.Int()
		}

		if v, ok := opts.Items.Get("performance/block_cache_size"); ok {
			cfg.PerformanceBlockCacheSize = v.Int()
		}

		if v, ok := opts.Items.Get("performance/max_open_files"); ok {
			cfg.PerformanceMaxOpenFiles = v.Int()
		}

		if v, ok := opts.Items.Get("performance/max_table_size"); ok {
			cfg.PerformanceMaxTableSize = v.Int()
		}
	}

	// Feature Settings
	{
		if v, ok := opts.Items.Get("feature/write_meta_disable"); ok && v.String() == "true" {
			cfg.FeatureWriteMetaDisable = true
		}

		if v, ok := opts.Items.Get("feature/write_log_disable"); ok && v.String() == "true" {
			cfg.FeatureWriteLogDisable = true
		}
	}

	// Cluster Settings
	{
		if v, ok := opts.Items.Get("cluster/masters"); ok {
			cfg.ClusterMasters = strings.Split(v.String(), ",")
		}

		if v, ok := opts.Items.Get("cluster/auth_secret_key"); ok {
			cfg.ClusterAuthSecretKey = v.String()
		}
	}

	return cfg.reset(), nil
}
