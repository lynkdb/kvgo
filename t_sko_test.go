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
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/sko"
)

var (
	dbCaches = map[string]*SkoConn{}
	dbMu     sync.Mutex
)

func init() {
	exec.Command("mkdir", "-p", "/dev/shm/kvgo").Output()
}

func dbOpen(ports []int) ([]*SkoConn, error) {

	dbMu.Lock()
	defer dbMu.Unlock()

	var (
		dbs   = []*SkoConn{}
		nodes = []string{}
	)

	for _, v := range ports {
		nodes = append(nodes, fmt.Sprintf("127.0.0.1:%d", v))
	}

	if len(ports) < 1 {
		ports = []int{0}
	}

	for _, port := range ports {

		test_dir := fmt.Sprintf("/dev/shm/kvgo/%d", port)

		if db, ok := dbCaches[test_dir]; ok {
			dbs = append(dbs, db)
			continue
		}

		if _, err := exec.Command("/usr/bin/rm", "-rf", test_dir).Output(); err != nil {
			return nil, err
		}

		//
		opts := connect.ConnOptions{}
		opts.SetValue("data_dir", test_dir)

		if len(nodes) > 0 {
			opts.SetValue("lynkdb/sko/cluster_bind", fmt.Sprintf("127.0.0.1:%d", port))
			opts.SetValue("lynkdb/sko/cluster_nodes", strings.Join(nodes, ","))
			opts.SetValue("lynkdb/sko/cluster_secret_key", "123456")
		}

		db, err := SkoOpen(opts)
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, db)

		dbCaches[test_dir] = db
	}

	return dbs, nil
}

func Test_Object_Common(t *testing.T) {

	for pn, ports := range [][]int{
		{},                    // local embedded
		{10001, 10002, 10003}, // cluster
	} {

		dbs, err := dbOpen(ports)
		if err != nil {
			t.Fatalf("Can Not Open Database %s", err.Error())
		}

		// Commit
		if rs := dbs[0].NewWriter([]byte("0001"), 1).Commit(); !rs.OK() {
			t.Fatalf("Commit ER!, Err %s", rs.Message)
		} else {
			t.Logf("Commit OK, Log %d", rs.Meta.Version)
		}

		// Query Key
		if rs := dbs[0].NewReader([]byte("0001")).Query(); !rs.OK() {
			t.Fatalf("Query Key ER! %s", rs.Message)
		} else {
			if rs.DataValue().Uint32() != 1 {
				t.Fatal("Query Key ER! Compare")
			} else {
				t.Logf("Query Key OK")
			}
		}

		// ObjectDel
		if rs := dbs[0].NewWriter([]byte("0001"), nil).ModeDeleteSet(true).Commit(); !rs.OK() {
			t.Fatal("ObjectDel ER!")
		}
		if rs := dbs[0].NewReader([]byte("0001")).Query(); !rs.NotFound() {
			t.Fatal("ObjectDel ER!")
		} else {
			t.Logf("ObjectDel Key OK")
		}

		//
		for _, n := range []int{1, 2, 3} {
			dbs[0].NewWriter([]byte(fmt.Sprintf("%04d", n)), n).Commit()
		}

		// Query KeyRange
		if rs := dbs[0].NewReader(nil).
			KeyRangeSet([]byte("0001"), []byte("0009")).
			LimitNumSet(10).Query(); !rs.OK() {
			t.Fatal("Query ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Query KeyRange ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if item.DataValue().Int() != (i + 2) {
					t.Fatal("Query KeyRange ER!")
				}
			}

			t.Logf("Query KeyRange OK")
		}

		// Query KeyRange+RevRange
		if rs := dbs[0].NewReader(nil).
			KeyRangeSet([]byte("0003"), []byte("0000")).
			ModeRevRangeSet(true).LimitNumSet(10).Query(); !rs.OK() {
			t.Fatal("Query RevRange ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Query RevRange ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if item.DataValue().Int() != (2 - i) {
					t.Fatal("Query RevRange ER!")
				}
			}

			t.Logf("Query KeyRange+RevRange OK")
		}

		// Commit Expired
		if rs := dbs[0].NewWriter([]byte("0001"), "ttl").
			ExpireSet(500).Commit(); !rs.OK() {
			t.Fatal("Commit ER! Expired")
		}

		if rs := dbs[0].NewReader([]byte("0001")).Query(); !rs.OK() {
			t.Fatal("Query Key ER! Expired")
		}
		time.Sleep(1e9)
		if rs := dbs[0].NewReader([]byte("0001")).Query(); !rs.NotFound() {
			t.Fatal("Query Key ER! Expired")
		} else {
			t.Logf("Commit Expirted OK")
		}

		// Commit IncrId
		key := []byte(fmt.Sprintf("incr-id-1-%d", pn))
		ow := sko.NewObjectWriter(key, "demo").
			IncrNamespaceSet("default")
		ow.Meta.IncrId = 1000
		if rs := dbs[0].Commit(ow); !rs.OK() {
			t.Fatal("Commit IncrId ER!")
		}
		if rs := dbs[0].NewReader(key).Query(); !rs.OK() || rs.Items[0].Meta.IncrId != 1000 {
			t.Fatal("Commit IncrId ER!")
		} else {
			t.Log("Commit IncrId OK")
		}
		key = []byte(fmt.Sprintf("incr-id-2-%d", pn))
		if rs := dbs[0].NewWriter(key, "demo").
			IncrNamespaceSet("default").Commit(); !rs.OK() {
			t.Fatal("Commit IncrId ER!")
		}
		if rs := dbs[0].NewReader(key).Query(); !rs.OK() || rs.Items[0].Meta.IncrId <= 1000 {
			t.Fatal("Commit IncrId ER!")
		} else {
			t.Log("Commit IncrId OK")
		}

		// Commit Struct
		obj := sko.ObjectData{
			Attrs: 100,
		}
		if rs := dbs[0].NewWriter([]byte("0001"), obj).Commit(); !rs.OK() {
			t.Fatal("Commit ER!")
		}
		if rs := dbs[0].NewReader([]byte("0001")).Query(); !rs.OK() {
			t.Fatal("Query Key ER!")
		} else {
			var item sko.ObjectData
			if err := rs.DataValue().Decode(&item, nil); err != nil {
				t.Fatalf("Query Key DataValue().Decode() ER! %s", err.Error())
			}
			if item.Attrs != 100 {
				t.Fatal("Query Key DataValue().Decode() ER!")
			}

			t.Logf("Commit Struct Encode/Decode OK")
		}
	}
}

func Test_Object_LogAsync(t *testing.T) {

	dbs, err := dbOpen([]int{20001, 20002, 20003})
	if err != nil {
		t.Fatalf("Can Not Open Database %s", err.Error())
	}

	var (
		key          = "log-async-key"
		value        = "log-async-test"
		cLog  uint64 = 0
	)

	ow := sko.NewObjectWriter([]byte(key), value)
	if rs := dbs[0].objectCommitLocal(ow, 0); !rs.OK() {
		t.Fatalf("Commit ER! %s", rs.Message)
	} else {
		cLog = rs.Meta.Version
		t.Logf("Commit OK cLog %d", cLog)
	}

	time.Sleep(4e9)

	ctx, fc := context.WithTimeout(context.Background(), time.Second*1)
	defer fc()

	rr := sko.NewObjectReader([]byte(key))

	for _, db := range dbs {

		for _, hp := range db.opts.ClusterNodes {

			conn, err := ClientConn(hp)
			if err != nil {
				t.Fatal("Object AsyncLog ER!")
			}

			rs, err := sko.NewObjectClient(conn).Query(ctx, rr)
			if err != nil {
				t.Fatal("Object AsyncLog ER!")
			}

			if !rs.OK() || len(rs.Items) == 0 {
				t.Fatal("Object AsyncLog ER!")
			}

			if rs.Items[0].Meta.Version != cLog {
				t.Fatalf("Object AsyncLog ER! %d/%d", rs.Items[0].Meta.Version, cLog)
			}

			// t.Logf("Object AsyncLog Bind %s, Node %s OK", db.opts.ClusterBind, hp)
		}

		t.Logf("Object AsyncLog Bind %s, Nodes %d OK",
			db.opts.ClusterBind, len(db.opts.ClusterNodes))
	}
}

func Benchmark_Commit_Seq(b *testing.B) {

	dbs, err := dbOpen(nil)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < b.N; i++ {
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand(b *testing.B) {

	dbs, err := dbOpen(nil)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < b.N; i++ {
		if rs := dbs[0].NewWriter(
			[]byte(fmt.Sprintf("%032d", rand.Int31())), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand_Cluster(b *testing.B) {

	dbs, err := dbOpen([]int{10001, 10002, 10003})
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < b.N; i++ {
		ow := sko.NewObjectWriter(
			[]byte(fmt.Sprintf("%032d", rand.Int31())), bs)
		if rs := dbs[rand.Intn(len(dbs))].Commit(ow); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Query_Key(b *testing.B) {

	dbs, err := dbOpen(nil)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 10000; i++ {
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, %d Err %s", i, rs.Message)
		}
	}

	for i := 0; i < b.N; i++ {
		if rs := dbs[0].NewReader([]byte(fmt.Sprintf("%032d", rand.Intn(1000)))).Query(); !rs.OK() {
			b.Fatalf("Query ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Query_KeyRange(b *testing.B) {

	dbs, err := dbOpen(nil)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < 10000; i++ {
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit Key ER! Err %s", rs.Message)
		}
	}

	for i := 0; i < b.N; i++ {
		offset := rand.Intn(10000 - 10)
		if rs := dbs[0].NewReader(nil).KeyRangeSet(
			[]byte(fmt.Sprintf("%032d", offset)),
			[]byte(fmt.Sprintf("%032d", offset+10))).
			LimitNumSet(10).Query(); !rs.OK() {
			b.Fatalf("Query KeyRange ER!, Err %s", rs.Message)
		}
	}
}
