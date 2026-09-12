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
	"strings"
	"testing"
	"time"

	"github.com/sysinner/innerstack/v2/pkg/inauth"

	"github.com/lynkdb/kvgo/v2/pkg/client"
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
		req := lynkapi.NewRequest("AdminService", "DatabaseCreate", &kvapi.DatabaseCreateRequest{
			Name:   "test",
			Engine: storage.DefaultDriver,
		})
		if rs := sess.ac.Exec(req); !rs.Status.OK() && rs.Status.Code != lynkapi.StatusCode_Conflict {
			t.Fatal(rs.Status.Err())
		} else if rs.Status.OK() {
			t.Logf("database create ok, meta %v", *rs.Data)
		}

		req = lynkapi.NewRequest("AdminService", "DatabaseList", &kvapi.DatabaseListRequest{})
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
		req := lynkapi.NewRequest("AdminService", "DatabaseUpdate", &kvapi.DatabaseUpdateRequest{
			Name:       "test",
			ReplicaNum: 2,
			Desc:       "test",
		})
		if rs := sess.ac.Exec(req); !rs.Status.OK() {
			t.Fatal(rs.Status.Err())
		} else {
			t.Logf("database alter ok : %v", *rs.Data)
		}

		req = lynkapi.NewRequest("AdminService", "DatabaseList", &kvapi.DatabaseListRequest{})
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

	// Auth gate (AdminService.PreMethod): a wrong/unregistered access key must
	// be rejected for every admin method, including SysInfo and DatabaseList.
	{
		wrongCli, err := (&lynkapi.ClientConfig{
			Addr:      sess.addr,
			AccessKey: inauth.NewAccessKey(), // random id/secret, not registered on the server
		}).NewClient()
		if err != nil {
			t.Fatal(err)
		}

		for _, method := range []string{"SysInfo", "DatabaseList", "StoreInfo"} {
			req := lynkapi.NewRequest("AdminService", method, &struct{}{})
			rs := wrongCli.Exec(req)
			if rs.Status.OK() {
				t.Fatalf("admin %s accepted with wrong access key — auth gate not enforced", method)
			}
			t.Logf("admin %s rejected with wrong key as expected: %s", method, rs.Status.Err())
		}
	}
}

// H-1 regression: the system database must not be readable or writable via
// the public data API with a non-admin (client scope) access key, while
// admin-scope keys keep full access.
func Test_ServiceApi_SystemDbAuth(t *testing.T) {

	sess, err := test_AdminApi_Open(t, StandaloneMode, "v2_vol_x", "dir=admin-api-sysauth")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	req := lynkapi.NewRequest("AdminService", "DatabaseCreate", &kvapi.DatabaseCreateRequest{
		Name:   "test_sysauth",
		Engine: storage.DefaultDriver,
	})
	if rs := sess.ac.Exec(req); !rs.Status.OK() && rs.Status.Code != lynkapi.StatusCode_Conflict {
		t.Fatal(rs.Status.Err())
	}

	// client-scoped key: granted kvgo/db only, not sys/all
	clientKey := inauth.NewAccessKey()
	clientKey.Roles = []string{"client"}
	clientKey.Scopes = []string{AuthScopeDatabase}
	if err := sess.dbs[0].keyMgr.Set(clientKey); err != nil {
		t.Fatal(err)
	}

	cliClient, err := (&client.Config{
		Addr:      sess.addr,
		AccessKey: clientKey,
	}).NewClient()
	if err != nil {
		t.Fatal(err)
	}

	cliAdmin, err := (&client.Config{
		Addr:      sess.addr,
		AccessKey: sess.dbs[0].cfg.Server.AccessKey,
	}).NewClient()
	if err != nil {
		t.Fatal(err)
	}

	sysKey := []byte("test/h1-system-db-auth")

	// client-scoped key must be denied on the system database
	{
		wr := kvapi.NewWriteRequest(sysKey, []byte("deny"))
		wr.Database = sysDatabaseName
		if rs := cliClient.Write(wr); rs.StatusCode != kvapi.Status_AuthDeny {
			t.Fatalf("client key write to system db not denied, status %d %s",
				rs.StatusCode, rs.ErrorMessage())
		}

		rd := kvapi.NewReadRequest(sysKey)
		rd.Database = sysDatabaseName
		if rs := cliClient.Read(rd); rs.StatusCode != kvapi.Status_AuthDeny {
			t.Fatalf("client key read from system db not denied, status %d %s",
				rs.StatusCode, rs.ErrorMessage())
		}

		rg := kvapi.NewRangeRequest(sysKey, append(bytesClone(sysKey), 0xff))
		rg.Database = sysDatabaseName
		if rs := cliClient.Range(rg); rs.StatusCode != kvapi.Status_AuthDeny {
			t.Fatalf("client key range on system db not denied, status %d %s",
				rs.StatusCode, rs.ErrorMessage())
		}

		dr := kvapi.NewDeleteRequest(sysKey)
		dr.Database = sysDatabaseName
		if rs := cliClient.Delete(dr); rs.StatusCode != kvapi.Status_AuthDeny {
			t.Fatalf("client key delete on system db not denied, status %d %s",
				rs.StatusCode, rs.ErrorMessage())
		}

		br := &kvapi.BatchRequest{
			Database: sysDatabaseName,
			Items: []*kvapi.RequestUnion{
				{Value: &kvapi.RequestUnion_Write{Write: kvapi.NewWriteRequest(sysKey, []byte("deny"))}},
			},
		}
		// Batch maps any valid() failure to Status_InvalidArgument, so assert
		// on the denial message instead of the status code.
		if rs := cliClient.Batch(br); rs.StatusCode == kvapi.Status_OK ||
			!strings.Contains(rs.ErrorMessage(), "access denied") {
			t.Fatalf("client key batch on system db not denied, status %d %s",
				rs.StatusCode, rs.ErrorMessage())
		}

		t.Logf("client key denied on system db as expected")
	}

	// the same client-scoped key works on a regular database
	{
		wr := kvapi.NewWriteRequest(sysKey, []byte("ok"))
		wr.Database = "test_sysauth"
		if rs := cliClient.Write(wr); !rs.OK() {
			t.Fatalf("client key write to regular db failed: %s", rs.ErrorMessage())
		}
	}

	// admin-scoped key (sys/all) keeps access to the system database
	{
		wr := kvapi.NewWriteRequest(sysKey, []byte("admin"))
		wr.Database = sysDatabaseName
		if rs := cliAdmin.Write(wr); !rs.OK() {
			t.Fatalf("admin key write to system db failed: %s", rs.ErrorMessage())
		}

		rd := kvapi.NewReadRequest(sysKey)
		rd.Database = sysDatabaseName
		if rs := cliAdmin.Read(rd); !rs.OK() || rs.Item() == nil ||
			string(rs.Item().Value) != "admin" {
			t.Fatalf("admin key read from system db failed: %s", rs.ErrorMessage())
		}

		dr := kvapi.NewDeleteRequest(sysKey)
		dr.Database = sysDatabaseName
		if rs := cliAdmin.Delete(dr); !rs.OK() {
			t.Fatalf("admin key delete on system db failed: %s", rs.ErrorMessage())
		}

		t.Logf("admin key allowed on system db as expected")
	}
}

type testAdminApiSession struct {
	dbs  []*dbServer
	dirs []string
	addr string
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
		opts    = map[string]bool{}
		dirName = "admin-api"
	)

	for _, arg := range args {
		switch arg.(type) {
		// 	case *testing.T:
		// 		t = arg.(*testing.T)

		case string:
			// "dir=<name>" selects an isolated data dir: the pebble driver
			// caches open connections by directory, so a closed session must
			// not be reopened on the same path within one test run.
			if s := arg.(string); strings.HasPrefix(s, "dir=") {
				dirName = s[len("dir="):]
			} else {
				opts[s] = true
			}
		}
	}

	port := 1024 + int(randUint64()%60000)

	testDir := "/tmp/kvgo-test/" + dirName
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
		testDir += "/kvgo-test/" + dirName
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
		addr: cfg.Server.Bind,
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
