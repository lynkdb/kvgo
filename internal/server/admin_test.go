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

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
	_ "github.com/lynkdb/kvgo/pkg/storage/pebble"
)

func Test_AdminAPI(t *testing.T) {

	sess, err := test_AdminApi_Open(t, StandaloneMode, "v2_vol_x")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		if rs := sess.ac.TableCreate(&kvapi.TableCreateRequest{
			Name:   "test",
			Engine: storage.DefaultDriver,
		}); !rs.OK() && rs.StatusCode != kvapi.Status_Conflict {
			t.Fatal(rs.StatusMessage)
		} else {
			t.Logf("table create ok, meta %v", rs.Meta())
		}

		if rs := sess.ac.TableList(&kvapi.TableListRequest{}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else if len(rs.Items) != 2 {
			t.Fatalf("table list issue %d", len(rs.Items))
		} else {
			t.Logf("table list ok")
		}
	}

	{
		time.Sleep(1e9)
		if rs := sess.ac.TableAlter(&kvapi.TableAlterRequest{
			Name:       "test",
			ReplicaNum: 2,
		}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else {
			t.Logf("table alter ok : %v", rs.Meta())
		}

		if rs := sess.ac.TableList(&kvapi.TableListRequest{}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else if len(rs.Items) != 2 {
			t.Fatal("table list issue")
		} else {
			t.Logf("table list ok")
		}
	}
}

type testAdminApiSession struct {
	dbs  []*dbServer
	dirs []string
	ac   kvapi.AdminClient
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

	cc := &ClientConfig{
		Addr:      fmt.Sprintf("127.0.0.1:%d", port),
		AccessKey: dbTestAccessKey,
	}

	sess.ac, err = cc.NewAdminClient()
	if err != nil {
		return nil, err
	}

	return sess, nil
}
