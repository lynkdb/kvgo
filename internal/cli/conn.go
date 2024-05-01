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
	"strings"

	"github.com/hooto/htoml4g/htoml"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/client"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

var (
	Prefix      string
	kvclient    kvapi.Client
	adminClient kvapi.AdminClient
	cfg         client.Config
	err         error
)

func Setup() error {

	if len(os.Args) == 2 && strings.HasSuffix(os.Args[1], ".toml") {

		if err = htoml.DecodeFromFile(os.Args[1], &cfg); err != nil {
			return err
		}

	} else {
		if Prefix, err = filepath.Abs(filepath.Dir(os.Args[0]) + "/.."); err != nil {
			Prefix = "/opt/lynkdb/kvgo/v2"
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
		cfg.Addr = srvConf.Server.Bind
		cfg.AccessKey = srvConf.Server.AccessKey
	}

	if _, _, err := net.SplitHostPort(cfg.Addr); err != nil {
		return err
	}

	if adminClient, err = cfg.NewAdminClient(); err != nil {
		return err
	}

	if kvclient, err = cfg.NewClient(); err != nil {
		return err
	}

	fmt.Printf("connect to %s\n", cfg.Addr)

	return nil
}
