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
	"bytes"
	"fmt"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/client"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
)

func Test_ServiceApi_RepX(t *testing.T) {

	const (
		test_ServiceApi_RepX_Database = "test_api_repx"
	)

	sess, err := test_ServiceApi_RepX_Open("repx")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		if rs := sess.ac.DatabaseCreate(&kvapi.DatabaseCreateRequest{
			Name:       test_ServiceApi_RepX_Database,
			Engine:     storage.DefaultDriver,
			ReplicaNum: 3,
		}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else {
			t.Logf("database create ok, meta %v", rs.Meta())
		}

		if rs := sess.ac.DatabaseList(&kvapi.DatabaseListRequest{}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else if len(rs.Items) != 2 {
			t.Fatalf("database list issue %d", len(rs.Items))
		} else {
			t.Logf("database list ok")
		}
	}

	sess.c.SetDatabase(test_ServiceApi_RepX_Database)

	var tm = sess.db.dbMapMgr.getByName(test_ServiceApi_RepX_Database)
	if tm == nil {
		t.Fatalf("setup database fail")
	}

	var shards = tm.lookupByRange([]byte{}, []byte{0xff}, false)
	if len(shards) != 1 || len(shards[0].replicas) != 3 {
		t.Fatalf("setup shards/replicas fail")
	}

	replicaClients := []kvapi.Client{}
	for _, rep := range shards[0].replicas {
		replicaClients = append(replicaClients, rep)
	}
	t.Logf("replicaClients %d", len(replicaClients))

	{
		testServiceApi(t, sess.c, replicaClients)
	}

	time.Sleep(2e9)
}

func testServiceApi(t *testing.T, c kvapi.Client, clients []kvapi.Client) {

	if len(clients) > 0 {
		clients = append([]kvapi.Client{c}, clients...)
	} else {
		clients = []kvapi.Client{c}
	}

	// Write
	{
		if rs := c.Write(kvapi.NewWriteRequest([]byte("0001"), []byte("1"))); !rs.OK() {
			t.Fatalf("Write ER!, Err %s", rs.ErrorMessage())
		} else {
			t.Logf("Write OK, Log %d", rs.Meta().Version)
		}
	}

	// Read-Key
	for _, c1 := range clients {
		if rs := c1.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.OK() {
			t.Fatalf("Read-Key ER! %s", rs.ErrorMessage())
		} else {
			if rs.Item().Uint64Value() != 1 {
				t.Fatal("Read-Key ER! Compare")
			} else if rs.MaxVersion < 100 {
				t.Fatal("Read-Key ER! MaxVersion")
			} else if bytes.Compare(rs.Item().Key, []byte("0001")) != 0 {
				t.Fatal("Read-Key ER! Key")
			} else {
				t.Logf("Read-Key OK, MaxVersion %d", rs.MaxVersion)
			}
		}
	}

	// Delete
	{
		if rs := c.Delete(kvapi.NewDeleteRequest([]byte("0001"))); !rs.OK() {
			t.Fatal("Delete-Key ER!")
		} else {
			t.Logf("Delete-Key Version %d", rs.Meta().Version)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.NotFound() {
				t.Fatal("Delete-ER!")
			} else {
				t.Logf("Delete-Key OK")
			}
		}
	}

	// Log
	{
		key := []byte("log-id-test")
		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("123"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else {
			t.Logf("Write LogId OK %d", rs.Meta().Version)
		}
		logID := uint64(0)
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); !rs.OK() {
				t.Fatal("Write LogId ER!")
			} else if rs.Meta().Version < 1 {
				t.Fatal("Write LogId ER!")
			} else {
				logID = rs.Meta().Version
				t.Logf("Read LogId OK %d", logID)
			}
		}

		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("123"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else {
			t.Logf("Write LogId OK %d", rs.Meta().Version)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); !rs.OK() {
				t.Fatal("Write LogId ER!")
			} else if rs.Meta().Version != logID {
				t.Fatalf("Write LogId ER! %d - %d", rs.Meta().Version, logID)
			}
		}

		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("321"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else {
			t.Logf("Write LogId OK %d", rs.Meta().Version)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); !rs.OK() {
				t.Fatal("Write LogId ER!")
			} else if rs.Meta().Version <= logID {
				t.Fatalf("Write LogId ER! %d - %d", rs.Meta().Version, logID)
			} else {
				t.Logf("Write LogId OK from %d to %d", logID, rs.Meta().Version)
			}
		}
	}

	//
	for _, n := range []int{1, 2, 3} {
		c.Write(kvapi.NewWriteRequest([]byte(fmt.Sprintf("%04d", n)), []byte(fmt.Sprintf("%v", n))))
	}

	// Range
	for _, c1 := range clients {
		req := &kvapi.RangeRequest{
			LowerKey: []byte("0001"),
			UpperKey: []byte("0009"),
			Limit:    10,
		}
		if rs := c1.Range(req); !rs.OK() {
			t.Fatal("Range ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range ER! %v", rs.Items)
				t.Fatalf("Range ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if len(item.Value) == 0 || item.Int64Value() != int64(i+2) {
					t.Fatal("Range ER!")
				}
			}

			t.Logf("Range OK")
		}

		req.Attrs = kvapi.Read_Attrs_MetaOnly
		if rs := c1.Range(req); !rs.OK() {
			t.Fatal("Range ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range ER! %d", len(rs.Items))
			}

			for _, item := range rs.Items {
				if item.Value != nil || len(item.Key) == 0 {
					t.Fatal("Range ER!")
				}
			}

			t.Logf("Range OK")
		}
	}

	// Range+RevRange
	for _, c1 := range clients {
		req := &kvapi.RangeRequest{
			LowerKey: []byte("0000"),
			UpperKey: []byte("0003"),
			Limit:    10,
			Revert:   true,
		}
		if rs := c1.Range(req); !rs.OK() {
			t.Fatal("Range+Rev ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range+Rev ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if len(item.Value) == 0 || item.Int64Value() != int64(2-i) {
					t.Logf("Range+Rev ER! #%d %v", 2-i, item.Value)
				}
			}

			t.Logf("Range+Rev OK")
		}
	}

	// Write Expired
	{
		for i1, c1 := range clients {
			if rs := c.Write(kvapi.NewWriteRequest([]byte("0001"), []byte("ttl")).SetTTL(100)); !rs.OK() {
				t.Fatal("Write Expirted ER!")
			}
			if rs := c1.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.OK() {
				t.Fatalf("Write Expirted ER! Read #%d", i1)
			}
			if rs := c1.Range(kvapi.NewRangeRequest([]byte("0000"), []byte("0001z"))); !rs.OK() {
				t.Fatal("Write Expirted ER! Range")
			}
			time.Sleep(100e6)
			if rs := c1.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.NotFound() {
				t.Fatalf("Write Expirted ER! Read %d, msg %v", i1, rs.StatusMessage)
			} else {
				t.Logf("Write Expirted OK")
			}
			if rs := c1.Range(kvapi.NewRangeRequest([]byte("0000"), []byte("0001z"))); !rs.NotFound() {
				t.Fatalf("Write Expirted ER! Range %v", rs)
			} else {
				t.Logf("Write Expirted OK")
			}
		}
	}

	round := 1

	// Write IncrId
	{
		var (
			incrNS = fmt.Sprintf("nsdef%d", round)
			key    = []byte(fmt.Sprintf("incr-id-1-%d", round))
		)

		req := kvapi.NewWriteRequest(key, []byte("demo"))

		req.IncrNamespace = incrNS
		req.Meta.IncrId = 1000

		if rs := c.Write(req); !rs.OK() {
			t.Fatalf("Write IncrId ER! %s", rs.ErrorMessage())
		} else {
			t.Logf("Write IncrId OK, incr-id %d", rs.Meta().IncrId)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); !rs.OK() || rs.Items[0].Meta.IncrId != 1000 {
				t.Fatalf("Read IncrId ER! %s", rs.ErrorMessage())
			} else {
				t.Logf("Read IncrId OK, incr-id %d", rs.Meta().IncrId)
			}
		}
		key = []byte(fmt.Sprintf("incr-id-2-%d", round))

		req = kvapi.NewWriteRequest(key, []byte("demo"))
		req.IncrNamespace = incrNS

		if rs := c.Write(req); !rs.OK() {
			t.Fatal("Write-2 IncrId ER!")
		} else {
			t.Logf("Write-2 IncrId OK, incr-id %d", rs.Meta().IncrId)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); !rs.OK() || rs.Items[0].Meta.IncrId <= 1000 {
				t.Fatal("Write-2 IncrId ER!")
			} else {
				t.Logf("Write-2 IncrId OK, incr-id %d", rs.Meta().IncrId)
			}
		}

		// Prev IncrId Check
		req = kvapi.NewWriteRequest(key, []byte("demo"))
		req.IncrNamespace = incrNS
		req.Meta.IncrId = 2000
		req.PrevIncrId = 1000
		if rs := c.Write(req); rs.OK() {
			t.Fatal("Write PrevIncrId ER!")
		} else {
			t.Logf("Write PrevIncrId OK, meta %v", rs.Meta())
		}
		req.PrevIncrId = 1001
		if rs := c.Write(req); rs.OK() {
			t.Logf("Write PrevIncrId OK, incr-id %d", rs.Meta().IncrId)
		} else {
			t.Fatalf("Write PrevIncrId ER! %s", rs.StatusMessage)
		}
		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); rs.OK() && rs.Meta().IncrId == req.Meta.IncrId {
				t.Log("Write PrevIncrId OK")
			} else {
				t.Fatalf("Write PrevIncrId Check ER! %d", rs.Meta().IncrId)
			}
		}
	}

	// Prev Attr Check
	{
		var (
			key = []byte(fmt.Sprintf("attr-id-1-%d", 1))
			req = kvapi.NewWriteRequest(key, []byte("demo"))
		)

		req.Meta.Attrs |= (1 << 48)
		req.Meta.Attrs |= (1 << 49)

		if rs := c.Write(req); rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}

		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); rs.OK() &&
				kvapi.AttrAllow(rs.Meta().Attrs, (1<<48)) &&
				kvapi.AttrAllow(rs.Meta().Attrs, (1<<49)) {
				t.Log("Write PrevAttrs OK")
			} else {
				t.Fatalf("Write PrevAttrs Check ER!")
			}
		}

		req = kvapi.NewWriteRequest(key, []byte("demo-1"))
		req.PrevAttrs |= (1 << 50)
		if rs := c.Write(req); !rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}
		req = kvapi.NewWriteRequest(key, []byte("demo-1"))
		req.PrevAttrs |= (1 << 48)
		if rs := c.Write(req); rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}

		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); rs.OK() &&
				kvapi.AttrAllow(rs.Items[0].Meta.Attrs, (1<<48)) &&
				kvapi.AttrAllow(rs.Items[0].Meta.Attrs, (1<<49)) {
				t.Log("Write PrevAttrs OK")
			} else {
				t.Fatalf("Write PrevAttrs Check ER!")
			}
		}
	}

	// Write Attrs Ignore Meta|Data
	{
		for _, va := range [][]uint64{
			{kvapi.Write_Attrs_IgnoreData, kvapi.Read_Attrs_MetaOnly, 0},
			{kvapi.Write_Attrs_IgnoreMeta, 0, kvapi.Read_Attrs_MetaOnly},
		} {

			key := []byte(fmt.Sprintf("data-off-%d-%d", round, va[0]))
			req := kvapi.NewWriteRequest(key, []byte("demo")).SetAttrs(va[0])
			if rs := c.Write(req); rs.OK() {
				t.Log("Write Attrs Ignore Meta|Data OK")
			} else {
				t.Fatal("Write Attrs Ignore Meta|Data ER!")
			}

			for _, c1 := range clients {
				if rs := c1.Read(kvapi.NewReadRequest(key).SetAttrs(va[1])); rs.OK() {
					t.Log("Write Attrs Ignore Meta|Data OK")
				} else {
					t.Fatal("Write Attrs Ignore Meta|Data ER!")
				}

				if rs := c1.Read(kvapi.NewReadRequest(key).SetAttrs(va[2])); rs.NotFound() {
					t.Log("Write Attrs Ignore Meta|Data OK")
				} else {
					t.Fatal("Write Attrs Ignore Meta|Data ER!")
				}
			}
		}
	}

	// Delete Data Only
	{
		var (
			key = []byte(fmt.Sprintf("delete-data-only-%d", round))
			req = kvapi.NewWriteRequest(key, []byte("demo"))
		)
		if rs := c.Write(req); rs.OK() {
			t.Log("Delete Attrs RetainMeta Step-1 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-1 ER!")
		}

		if rs := c.Delete(kvapi.NewDeleteRequest(key).SetRetainMeta(true)); rs.OK() {
			t.Log("Delete Attrs RetainMeta Step-2 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-2 ER!")
		}

		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); rs.NotFound() {
				t.Log("Delete Attrs RetainMeta Step-3 OK")
			} else {
				t.Fatal("Delete Attrs RetainMeta Step-3 ER!")
			}

			if rs := c1.Read(kvapi.NewReadRequest(key).SetMetaOnly(true)); rs.OK() {
				t.Logf("Delete Attrs RetainMeta Step-4 OK")
			} else {
				t.Fatal("Delete Attrs RetainMeta Step-4 ER!")
			}
		}
	}

	// Write Meta Extra
	{
		var (
			extra = []byte("extra-data")
			key   = []byte("meta-extra-data")
			value = []byte("demo")
			req   = kvapi.NewWriteRequest(key, value)
		)

		req.Meta.Extra = extra
		if rs := c.Write(req); rs.OK() {
			t.Log("Write Meta Extra OK")
		} else {
			t.Fatal("Write Meta Extra ER!")
		}

		for _, c1 := range clients {
			if rs := c1.Read(kvapi.NewReadRequest(key)); rs.OK() &&
				bytes.Compare(rs.Item().Value, value) == 0 &&
				bytes.Compare(extra, rs.Meta().Extra) == 0 {
				t.Log("Write Meta Extra OK")
			} else {
				t.Fatal("Write Meta Extra ER!")
			}

			if rs := c1.Read(kvapi.NewReadRequest(key).SetMetaOnly(true)); rs.OK() &&
				rs.Item() != nil &&
				len(rs.Item().Value) == 0 &&
				bytes.Compare(extra, rs.Meta().Extra) == 0 {
				t.Log("Write Meta Extra OK")
			} else {
				t.Fatal("Write Meta Extra ER!")
			}
		}
	}
}

type testServiceApiRepXSession struct {
	dirs []string
	db   *dbServer
	ac   kvapi.AdminClient
	c    kvapi.Client
}

func (it *testServiceApiRepXSession) release() {
	it.db.Close()
	for _, dir := range it.dirs {
		exec.Command("rm", "-rf", dir).Output()
		testPrintf("rm %s", dir)
	}
}

var (
	testServiceApiMut sync.Mutex
)

func test_ServiceApi_RepX_Open(name string, args ...interface{}) (*testServiceApiRepXSession, error) {

	testServiceApiMut.Lock()
	defer testServiceApiMut.Unlock()

	test_ServiceApi_RepX_AccessKey := NewSystemAccessKey()

	port := 1024 + mrand.Intn(60000)

	var (
		opts = map[string]bool{}
		cc   = &client.ClientConfig{
			Addr:      fmt.Sprintf("127.0.0.1:%d", port),
			AccessKey: test_ServiceApi_RepX_AccessKey,
		}
	)

	for _, arg := range args {
		switch arg.(type) {
		case string:
			opts[arg.(string)] = true
		}
	}

	testDir := "/tmp/kvgo-test"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
		testDir += "/kvgo-test"
	}
	testDir = filepath.Clean(fmt.Sprintf("%s/service-api-%s", testDir, name))

	if _, err := exec.Command("rm", "-rf", testDir).Output(); err != nil {
		return nil, err
	}

	sess := &testServiceApiRepXSession{
		dirs: []string{testDir},
	}

	//
	cfg := NewConfig(testDir)

	cfg.Storage.DataDirectory = testDir
	cfg.Server.Bind = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.Server.AccessKey = test_ServiceApi_RepX_AccessKey
	cfg.Server.RuntimeMode = StandaloneMode

	if _, ok := opts["compression_with_none"]; ok {
		cfg.Feature.Compression = "none"
	}

	for i := 0; i < 4; i++ {
		dir := fmt.Sprintf("%s/vol-%02d", testDir, i)
		exec.Command("rm", "-rf", dir).Output()
		cfg.Storage.Stores = append(cfg.Storage.Stores, &ConfigStore{
			Engine:     storage.DefaultDriver,
			Mountpoint: dir,
		})
		exec.Command("mkdir", "-p", dir).Output()
		sess.dirs = append(sess.dirs, dir)
	}

	db, err := dbServerSetup(fmt.Sprintf("%s/%s-server.toml", testDir, name), *cfg)
	if err != nil {
		return nil, err
	}
	sess.db = db

	client, err := cc.NewClient()
	if err != nil {
		return nil, err
	}
	sess.c = client

	ac, err := cc.NewAdminClient()
	if err != nil {
		return nil, err
	}
	sess.ac = ac

	return sess, nil
}
