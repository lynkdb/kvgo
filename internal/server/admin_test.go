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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
	"github.com/lynkdb/lynkapi/go/lynkapi"
)

func Test_AdminAPI(t *testing.T) {

	sess, err := test_AdminApi_Open(t, StandaloneMode, "v2_vol_x")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		req, _ := lynkapi.NewRequestFromObject("AdminService", "DatabaseCreate", &kvapi.DatabaseCreateRequest{
			Name:   "test",
			Engine: storage.DefaultDriver,
		})
		if rs := sess.ac.Exec(req); !rs.Status.OK() && rs.Status.Code != lynkapi.StatusCode_Conflict {
			t.Fatal(rs.Status.Err())
		} else if rs.Status.OK() {
			t.Logf("database create ok, meta %v", *rs.Data)
		}

		req, _ = lynkapi.NewRequestFromObject("AdminService", "DatabaseList", &kvapi.DatabaseListRequest{})
		if rs := sess.ac.Exec(req); !rs.Status.OK() {
			t.Fatal(rs.Status.Err())
		} else {
			var data kvapi.DatabaseListResponse
			if err := rs.Decode(&data); err != nil {
				t.Fatal(err)
			}
			if len(data.Items) != 2 {
				t.Fatalf("database list issue %d", len(data.Items))
			} else {
				t.Logf("database list ok")
			}
		}
	}

	{
		time.Sleep(1e9)
		req, _ := lynkapi.NewRequestFromObject("AdminService", "DatabaseUpdate", &kvapi.DatabaseUpdateRequest{
			Name:       "test",
			ReplicaNum: 2,
			Desc:       "test",
		})
		if rs := sess.ac.Exec(req); !rs.Status.OK() {
			t.Fatal(rs.Status.Err())
		} else {
			t.Logf("database alter ok : %v", *rs.Data)
		}

		req, _ = lynkapi.NewRequestFromObject("AdminService", "DatabaseList", &kvapi.DatabaseListRequest{})
		if rs := sess.ac.Exec(req); !rs.Status.OK() {
			t.Fatal(rs.Status.Err())
		} else {
			var data kvapi.DatabaseListResponse
			if err := rs.Decode(&data); err != nil {
				t.Fatal(err)
			}
			if len(data.Items) != 2 {
				t.Fatalf("database list issue %d", len(data.Items))
			} else {
				t.Logf("database list ok")
			}
		}
	}
}

type testAdminApiSession struct {
	dbs  []*dbServer
	dirs []string
	// ac   kvapi.AdminClient
	ac lynkapi.Client
}

func (it *testAdminApiSession) release() {
	for _, db := range it.dbs {
		db.Close()
	}
	for _, dir := range it.dirs {
		exec.Command("rm", "-rf", dir).Output()
	}
}

func test_AdminApi_Open(args ...interface{}) (*testAdminApiSession, error) {

	var (
		opts = map[string]bool{}
	)

	for _, arg := range args {
		switch arg.(type) {
		// 	case *testing.T:
		// 		t = arg.(*testing.T)

		case string:
			opts[arg.(string)] = true
		}
	}

	port := 1024 + int(randUint64()%60000)

	testDir := "/tmp/kvgo-test/admin-api"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
		testDir += "/kvgo-test/admin-api"
	}
	testDir = filepath.Clean(testDir)

	if _, err := exec.Command("rm", "-rf", testDir).Output(); err != nil {
		return nil, err
	}

	dbTestAccessKey := NewSystemAccessKey()

	//
	cfg := NewConfig(testDir)

	cfg.Storage.DataDirectory = testDir
	cfg.Server.Bind = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.Server.AccessKey = dbTestAccessKey

	if opts[StandaloneMode] {
		cfg.Server.RuntimeMode = StandaloneMode
	}

	sess := &testAdminApiSession{
		dirs: []string{testDir},
	}

	if opts["v2_vol_x"] {
		for i := 0; i < 3; i++ {
			dir := fmt.Sprintf("%s/vol-%02d", testDir, i)
			exec.Command("rm", "-rf", dir).Output()
			cfg.Storage.Stores = append(cfg.Storage.Stores, &ConfigStore{
				Engine:     storage.DefaultDriver,
				Mountpoint: dir,
			})
			exec.Command("mkdir", "-p", dir).Output()
			sess.dirs = append(sess.dirs, dir)
		}
	}

	db, err := dbServerSetup(testDir+"/server.toml", *cfg)
	if err != nil {
		return nil, err
	}
	sess.dbs = append(sess.dbs, db)

	// cc := &client.Config{
	// 	Addr:      fmt.Sprintf("127.0.0.1:%d", port),
	// 	AccessKey: dbTestAccessKey,
	// }

	// sess.ac, err = cc.NewAdminClient()
	// if err != nil {
	// 	return nil, err
	// }

	cc := &lynkapi.ClientConfig{
		Addr:      fmt.Sprintf("127.0.0.1:%d", port),
		AccessKey: dbTestAccessKey,
	}
	sess.ac, err = cc.NewClient()
	if err != nil {
		return nil, err
	}

	return sess, nil
}
