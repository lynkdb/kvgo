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

package cli

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/hooto/htoml4g/htoml"

	"github.com/lynkdb/kvgo/internal/server"
	"github.com/lynkdb/kvgo/pkg/kvapi"
)

var (
	Prefix      string
	adminClient kvapi.AdminClient
	mu          sync.RWMutex
	cfg         server.ClientConfig
	err         error
)

func Setup() error {

	{
		if Prefix, err = filepath.Abs(filepath.Dir(os.Args[0]) + "/.."); err != nil {
			Prefix = "/opt/lynkdb/kvgo"
		}

		var (
			confFile = Prefix + "/etc/kvgo-server.toml"
			srvConf  server.Config
		)

		if err = htoml.DecodeFromFile(confFile, &srvConf); err != nil {
			srvConf.Server.Bind = "127.0.0.1:9566"
			return err
		}

		if srvConf.Server.AccessKey == nil {
			return fmt.Errorf("access-key not found (%s)", confFile)
		}
		cfg.AccessKey = srvConf.Server.AccessKey

		if _, port, err := net.SplitHostPort(srvConf.Server.Bind); err != nil {
			return err
		} else {
			cfg.Addr = fmt.Sprintf("127.0.0.1:%v", port)
		}

		if adminClient, err = cfg.NewAdminClient(); err != nil {
			return err
		}

		fmt.Printf("connect to %s\n", cfg.Addr)
	}

	return nil
}
