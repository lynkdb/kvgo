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

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

var (
	dbTestMu        sync.Mutex
	dbTestCaches    = map[string]*Conn{}
	dbTestAccessKey = NewSystemAccessKey()
)

func dbOpen(ports []int, clientEnable bool) ([]*Conn, error) {

	dbTestMu.Lock()
	defer dbTestMu.Unlock()

	var (
		dbs   = []*Conn{}
		nodes = []*ClientConfig{}
	)

	for _, v := range ports {
		if v > 0 {
			nodes = append(nodes, &ClientConfig{
				Addr:      fmt.Sprintf("127.0.0.1:%d", v),
				AccessKey: dbTestAccessKey,
			})
		}
	}

	if clientEnable {

		if len(nodes) < 1 {
			return nil, fmt.Errorf("no nodes")
		}

		cfg := &Config{}

		cfg.Cluster.MainNodes = nodes
		cfg.ClientConnectEnable = clientEnable

		db, err := Open(cfg)
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, db)

		return dbs, nil
	}

	if len(ports) < 1 {
		ports = []int{0}
	}

	for _, port := range ports {

		test_dir := fmt.Sprintf("/dev/shm/kvgo/%d", port)

		if db, ok := dbTestCaches[test_dir]; ok {
			dbs = append(dbs, db)
			continue
		}

		if _, err := exec.Command("rm", "-rf", test_dir).Output(); err != nil {
			return nil, err
		}

		//
		cfg := NewConfig(test_dir)

		if port > 0 {
			cfg.Server.Bind = fmt.Sprintf("127.0.0.1:%d", port)
			cfg.Server.AccessKey = dbTestAccessKey
		}

		if port < 0 {
			cfg.Feature.WriteMetaDisable = true
			cfg.Feature.WriteLogDisable = true
		}

		if len(nodes) > 0 {
			cfg.Cluster.MainNodes = nodes
		}

		db, err := Open(cfg)
		if err != nil {
			return nil, err
		}

		dbs = append(dbs, db)

		dbTestCaches[test_dir] = db
	}

	return dbs, nil
}

func Test_Object_Common(t *testing.T) {

	round := 0

	for pn, ports := range [][]int{
		{},                    // local embedded
		{10001, 10002, 10003}, // cluster
	} {

		dbs, err := dbOpen(ports, false)
		if err != nil {
			t.Fatalf("Can Not Open Database %s", err.Error())
		}

		dbt := []*Conn{dbs[0]}

		if len(ports) > 1 {
			if dbs2, err := dbOpen(ports, true); err != nil {
				t.Fatalf("Can Not Open Database %s", err.Error())
			} else {
				dbt = append(dbt, dbs2[0])
			}
		}

		for _, db := range dbt {

			round += 1
			t.Logf("ROUND #%d", round)

			// Commit
			{
				if rs := db.NewWriter([]byte("0001"), 1).Commit(); !rs.OK() {
					t.Fatalf("Commit ER!, Err %s", rs.Message)
				} else {
					t.Logf("Commit OK, Log %d", rs.Meta.Version)
				}
			}

			// Query Key
			{
				if rs := db.NewReader([]byte("0001")).Query(); !rs.OK() {
					t.Fatalf("Query Key ER! %d, %s", rs.Status, rs.Message)
				} else {
					if rs.DataValue().Uint32() != 1 {
						t.Fatal("Query Key ER! Compare")
					} else {
						t.Logf("Query Key OK")
					}
				}
			}

			// ObjectDel
			{
				if rs := db.NewWriter([]byte("0001"), nil).ModeDeleteSet(true).Commit(); !rs.OK() {
					t.Fatal("ObjectDel ER!")
				}
				if rs := db.NewReader([]byte("0001")).Query(); !rs.NotFound() {
					t.Fatal("ObjectDel ER!")
				} else {
					t.Logf("ObjectDel Key OK")
				}
			}

			// Log
			{
				key := []byte("log-id-test")
				if rs := db.NewWriter(key, 123).Commit(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				}
				logId := uint64(0)
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version < 1 {
					t.Fatal("Object LogId ER!")
				} else {
					logId = rs.Meta.Version
				}

				if rs := db.NewWriter(key, 123).Commit(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				}
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version != logId {
					t.Fatal("Object LogId ER!")
				}

				if rs := db.NewWriter(key, 321).Commit(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				}
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version <= logId {
					t.Fatal("Object LogId ER!")
				} else {
					t.Logf("Object LogId OK from %d to %d", logId, rs.Meta.Version)
				}
			}

			//
			for _, n := range []int{1, 2, 3} {
				db.NewWriter([]byte(fmt.Sprintf("%04d", n)), n).Commit()
			}

			// Query KeyRange
			{
				if rs := db.NewReader(nil).
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
			}

			// Query KeyRange+RevRange
			{
				if rs := db.NewReader(nil).
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
			}

			// Commit Expired
			{
				if rs := db.NewWriter([]byte("0001"), "ttl").
					ExpireSet(500).Commit(); !rs.OK() {
					t.Fatal("Commit ER! Expired")
				}

				if rs := db.NewReader([]byte("0001")).Query(); !rs.OK() {
					t.Fatal("Query Key ER! Expired")
				}
				time.Sleep(1e9)
				if rs := db.NewReader([]byte("0001")).Query(); !rs.NotFound() {
					t.Fatal("Query Key ER! Expired")
				} else {
					t.Logf("Commit Expirted OK")
				}
			}

			// Commit IncrId
			{
				incrNS := fmt.Sprintf("nsdef%d", round)
				key := []byte(fmt.Sprintf("incr-id-1-%d", round))
				ow := kv2.NewObjectWriter(key, "demo").
					IncrNamespaceSet(incrNS)
				ow.Meta.IncrId = 1000
				if rs := db.Commit(ow); !rs.OK() {
					t.Fatalf("Commit IncrId ER! %s", rs.Message)
				}
				if rs := db.NewReader(key).Query(); !rs.OK() || rs.Items[0].Meta.IncrId != 1000 {
					t.Fatalf("Commit IncrId ER! %s", rs.Message)
				} else {
					t.Log("Commit IncrId OK")
				}
				key = []byte(fmt.Sprintf("incr-id-2-%d", round))
				if rs := db.NewWriter(key, "demo").
					IncrNamespaceSet(incrNS).Commit(); !rs.OK() {
					t.Fatal("Commit IncrId ER!")
				}
				if rs := db.NewReader(key).Query(); !rs.OK() || rs.Items[0].Meta.IncrId <= 1000 {
					t.Fatal("Commit IncrId ER!")
				} else {
					t.Logf("Commit IncrId OK, incr_id %d", rs.Items[0].Meta.IncrId)
				}

				// Prev IncrId Check
				ow = kv2.NewObjectWriter(key, "demo").
					IncrNamespaceSet(incrNS)
				ow.Meta.IncrId = 2000
				ow.PrevIncrId = 1000
				if rs := db.Commit(ow); rs.OK() {
					t.Fatal("Commit PrevIncrId ER!")
				} else {
					t.Log("Commit PrevIncrId OK")
				}
				ow.PrevIncrId = 1001
				if rs := db.Commit(ow); rs.OK() {
					t.Log("Commit PrevIncrId OK")
				} else {
					t.Fatal("Commit PrevIncrId ER!")
				}
				if rs := db.NewReader(key).Query(); rs.OK() && rs.Items[0].Meta.IncrId == ow.Meta.IncrId {
					t.Log("Commit PrevIncrId OK")
				} else {
					t.Fatalf("Commit PrevIncrId Check ER!")
				}
			}

			// Prev Attr Check
			{
				key := []byte(fmt.Sprintf("attr-id-1-%d", pn))
				ow := kv2.NewObjectWriter(key, "demo")
				ow.Meta.Attrs |= (1 << 48)
				ow.Meta.Attrs |= (1 << 49)
				if rs := db.Commit(ow); rs.OK() {
					t.Log("Commit PrevAttrs OK")
				} else {
					t.Fatal("Commit PrevAttrs ER!")
				}

				if rs := db.NewReader(key).Query(); rs.OK() &&
					kv2.AttrAllow(rs.Items[0].Meta.Attrs, (1<<48)) &&
					kv2.AttrAllow(rs.Items[0].Meta.Attrs, (1<<49)) {
					t.Log("Commit PrevAttrs OK")
				} else {
					t.Fatalf("Commit PrevAttrs Check ER!")
				}

				ow = kv2.NewObjectWriter(key, "demo-1")
				ow.PrevAttrs |= (1 << 50)
				if rs := db.Commit(ow); !rs.OK() {
					t.Log("Commit PrevAttrs OK")
				} else {
					t.Fatal("Commit PrevAttrs ER!")
				}
				ow = kv2.NewObjectWriter(key, "demo-1")
				ow.PrevAttrs |= (1 << 48)
				if rs := db.Commit(ow); rs.OK() {
					t.Log("Commit PrevAttrs OK")
				} else {
					t.Fatal("Commit PrevAttrs ER!")
				}

				if rs := db.NewReader(key).Query(); rs.OK() &&
					kv2.AttrAllow(rs.Items[0].Meta.Attrs, (1<<48)) &&
					kv2.AttrAllow(rs.Items[0].Meta.Attrs, (1<<49)) {
					t.Log("Commit PrevAttrs OK")
				} else {
					t.Fatalf("Commit PrevAttrs Check ER!")
				}
			}

			// MetaAttr On/Off
			{
				for _, va := range [][]uint64{
					{kv2.ObjectMetaAttrMetaOff, kv2.ObjectMetaAttrDataOff},
					{kv2.ObjectMetaAttrDataOff, kv2.ObjectMetaAttrMetaOff},
				} {

					key := []byte(fmt.Sprintf("data-off-%d-%d", round, va[0]))
					ow := kv2.NewObjectWriter(key, "demo")
					ow.Meta.Attrs |= va[0]
					if rs := db.Commit(ow); rs.OK() {
						t.Log("MetaAttr On/Off OK")
					} else {
						t.Fatal("MetaAttr On/Off ER!")
					}

					if rs := db.NewReader(key).AttrSet(va[0]).Query(); rs.OK() {
						t.Log("MetaAttr On/Off OK")
					} else {
						t.Fatal("MetaAttr On/Off ER!")
					}

					if rs := db.NewReader(key).AttrSet(va[1]).Query(); rs.OK() {
						t.Fatal("MetaAttr On/Off ER!")
					} else {
						t.Log("MetaAttr On/Off OK")
					}
				}
			}

			// Commit Struct
			{
				obj := kv2.ObjectData{
					Attrs: 100,
				}
				if rs := db.NewWriter([]byte("0001"), obj).Commit(); !rs.OK() {
					t.Fatal("Commit ER!")
				}
				if rs := db.NewReader([]byte("0001")).Query(); !rs.OK() {
					t.Fatalf("Query Key ER! status %d", rs.Status)
				} else {
					var item kv2.ObjectData
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
	}
}

func Test_Object_LogAsync(t *testing.T) {

	dbs, err := dbOpen([]int{20001, 20002, 20003}, false)
	if err != nil {
		t.Fatalf("Can Not Open Database %s", err.Error())
	}
	time.Sleep(1e9)

	var (
		key   = "log-async-key"
		value = "log-async-test"
		attrs = [][]uint64{
			{0, 0},
			{kv2.ObjectMetaAttrMetaOff, 0},
			{kv2.ObjectMetaAttrDataOff, 0},
		}
	)

	for i, va := range attrs {
		ow := kv2.NewObjectWriter([]byte(fmt.Sprintf("%s-%d", key, va[0])), value)
		ow.Meta.Attrs |= va[0]
		if rs := dbs[0].commitLocal(ow, 0); !rs.OK() {
			t.Fatalf("Commit ER! %s", rs.Message)
		} else {
			attrs[i][1] = rs.Meta.Version
			t.Logf("Commit OK cLog %d", attrs[i][1])
		}
	}

	ctx, fc := context.WithTimeout(context.Background(), time.Second*1)
	defer fc()

	for _, db := range dbs {

		for _, hp := range db.opts.Cluster.MainNodes {

			conn, err := clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, false)
			if err != nil {
				t.Fatalf("Object AsyncLog ER %s", err.Error())
			}

			for _, va := range attrs {

				rr := kv2.NewObjectReader([]byte(fmt.Sprintf("%s-%d", key, va[0])))
				rr.Attrs |= va[0]

				rs, err := kv2.NewPublicClient(conn).Query(ctx, rr)
				if err != nil {
					t.Fatal("Object AsyncLog ER")
				}

				if !rs.OK() || len(rs.Items) == 0 {
					t.Fatalf("Object AsyncLog ER, ok %v, msg %s, len %d, node %s",
						rs.OK(), rs.Message, len(rs.Items), hp.Addr)
				} else {
					t.Logf("Object AsyncLog OK, node %s", hp.Addr)
				}

				if rs.Items[0].Meta.Version != va[1] {
					t.Fatalf("Object AsyncLog ER %d/%d", rs.Items[0].Meta.Version, va[1])
				}
			}

			// t.Logf("Object AsyncLog Bind %s, Node %s OK", db.opts.Server.Bind, hp)
		}

		t.Logf("Object AsyncLog Bind %s, MainNodes %d OK",
			db.opts.Server.Bind, len(db.opts.Cluster.MainNodes))
	}
}

func Test_Table(t *testing.T) {

	dbs, err := dbOpen([]int{}, false)
	if err != nil {
		t.Fatalf("Can Not Open Database %s", err.Error())
	}

	// TableSet
	rs := dbs[0].SysCmd(kv2.NewSysCmdRequest("TableSet", &kv2.TableSetRequest{
		Name: "demo",
		Desc: "desc ...",
	}))
	if !rs.OK() {
		t.Fatalf(rs.Message)
	}

	// TableList
	rs = dbs[0].SysCmd(kv2.NewSysCmdRequest("TableList", &kv2.TableListRequest{}))
	if !rs.OK() {
		t.Fatalf(rs.Message)
	}

	if len(rs.Items) != 2 {
		t.Fatalf("invalid num of tables")
	}

	// Commit
	if rs := dbs[0].NewWriter([]byte("t0001"), 1).TableNameSet("demo").Commit(); !rs.OK() {
		t.Fatalf("Commit ER!, Err %s", rs.Message)
	} else {
		t.Logf("Table Commit OK, Log %d", rs.Meta.Version)
	}

	// Query Key
	if rs := dbs[0].NewReader([]byte("t0001")).TableNameSet("demo").Query(); !rs.OK() {
		t.Fatalf("Table Query Key ER! %d, %s", rs.Status, rs.Message)
	} else {
		if rs.DataValue().Uint32() != 1 {
			t.Fatal("Table Query Key ER! Compare")
		} else {
			t.Logf("Table Query Key OK")
		}
	}

	// Query Key
	if rs := dbs[0].NewReader([]byte("t0001")).TableNameSet("main").Query(); !rs.NotFound() {
		t.Fatal("Table Query Key ER")
	} else {
		t.Logf("Table Query Key OK")
	}
}

func Benchmark_Commit_Seq(b *testing.B) {

	dbs, err := dbOpen(nil, false)
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

func Benchmark_Commit_Seq_MetaLogDisable(b *testing.B) {

	dbs, err := dbOpen([]int{-1}, false)
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

	dbs, err := dbOpen(nil, false)
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

func Benchmark_Commit_Rand_MetaLogDisable(b *testing.B) {

	dbs, err := dbOpen([]int{-2}, false)
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

func Benchmark_Commit_Rand_Cluster_x3_1k(b *testing.B) {

	dbs, err := dbOpen([]int{10001, 10002, 10003}, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 1000))
	for i := 0; i < b.N; i++ {
		ow := kv2.NewObjectWriter(
			[]byte(fmt.Sprintf("%032d", rand.Int31())), bs)
		if rs := dbs[rand.Intn(len(dbs))].Commit(ow); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand_Cluster_x3_100b(b *testing.B) {

	dbs, err := dbOpen([]int{11001, 11002, 11003}, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	bs := []byte(strings.Repeat("a", 100))
	for i := 0; i < b.N; i++ {
		ow := kv2.NewObjectWriter(
			[]byte(fmt.Sprintf("%032d", rand.Int31())), bs)
		if rs := dbs[rand.Intn(len(dbs))].Commit(ow); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Query_Key(b *testing.B) {

	dbs, err := dbOpen(nil, false)
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

	dbs, err := dbOpen(nil, false)
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
