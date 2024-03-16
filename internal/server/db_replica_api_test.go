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

package server_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/internal/tests"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func Test_DatabaseReplica_API(t *testing.T) {
	//
	sess, err := test_DatabaseReplica_API_Open(storage.DefaultDriver, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	db, err := server.NewDatabase(sess.db, "0001", "db_api_test", 1, 2, &server.Config{})
	if err != nil {
		t.Fatal(err)
	}

	tests.ClientAPI(db, t)
}

type testDatabaseReplicaApiSession struct {
	dir string
	db  storage.Conn
}

func (it *testDatabaseReplicaApiSession) release() {
	it.db.Close()
	exec.Command("rm", "-rf", it.dir).Output()
}

func test_DatabaseReplica_API_Open(drvname string, samples int) (*testDatabaseReplicaApiSession, error) {

	testDir := "/tmp/kvgo-test"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
		testDir += "/kvgo-test"
	}

	if samples > 0 {
		testDir = filepath.Clean(fmt.Sprintf("%s/db-replica-api-%d", testDir, samples))
	} else {
		testDir = filepath.Clean(fmt.Sprintf("%s/db-replica-api", testDir))
	}

	db, err := storage.Open(drvname, &storage.Options{
		DataDirectory:   testDir,
		WriteBufferSize: 16,
	})
	if err != nil {
		return nil, err
	}

	if samples > 0 {
		// if rs := db.Get([]byte(fmt.Sprintf("%032d", samples-1)), nil); rs.NotFound() {
		// 	for i := 0; i < samples; i++ {
		// 		bs := randBytes(128 + mrand.Intn(256)) // size 128 ~ 384 bytes, avg 256 bytes
		// 		if rs := db.Put([]byte(fmt.Sprintf("%032d", i)), bs, nil); !rs.OK() {
		// 			return nil, rs.Error()
		// 		}
		// 	}
		// 	fmt.Println("storage samples", samples)
		// }
	}

	sess := &testDatabaseReplicaApiSession{
		dir: testDir,
		db:  db,
	}

	return sess, nil
}
