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
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

var (
	dbTestMu        sync.Mutex
	dbTestCaches    = map[string]*Conn{}
	dbTestAccessKey = NewSystemAccessKey()
)

func dbTestOpen(ports []int, clientEnable bool) ([]*Conn, error) {

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

		testDir := "/dev/shm"
		if runtime.GOOS == "darwin" {
			testDir, _ = os.UserHomeDir()
		}
		testDir = filepath.Clean(fmt.Sprintf("%s/kvgo/%d", testDir, port))

		if db, ok := dbTestCaches[testDir]; ok {
			dbs = append(dbs, db)
			continue
		}

		if _, err := exec.Command("rm", "-rf", testDir).Output(); err != nil {
			return nil, err
		}

		//
		cfg := NewConfig(testDir)

		cfg.Performance.WriteBufferSize = 8 // MiB
		cfg.Performance.BlockCacheSize = 8  // MiB
		cfg.Performance.MaxTableSize = 8    // MiB

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

		dbTestCaches[testDir] = db
	}

	return dbs, nil
}

var (
	dbTestSample *Conn
	dbTestSMu    sync.Mutex
	dataSamples  [][]byte
)

func init() {
	if strings.Contains(strings.Join(os.Args, ","), "test.bench") {

		dataSamples = make([][]byte, 10000)
		for i := 0; i < 10000; i++ {
			dataSamples[i] = randBytes(1000)
		}

		fmt.Println("samples", len(dataSamples))

		dbSample(10000)
	}
}

func randBytes(size int) []byte {

	if size < 1 {
		size = 1
	} else if size > 2000000 {
		size = 2000000
	}

	bs := make([]byte, size)

	if _, err := crand.Read(bs); err != nil {
		for i := range bs {
			bs[i] = uint8(mrand.Intn(256))
		}
	}

	return bs
}

func dataSample() []byte {
	return dataSamples[mrand.Intn(len(dataSamples))]
}

func dbSample(keys int) (*Conn, error) {
	dbTestSMu.Lock()
	defer dbTestSMu.Unlock()

	if dbTestSample != nil {
		return dbTestSample, nil
	}

	dbs, err := dbTestOpen([]int{-10}, false)
	if err != nil {
		return nil, err
	}

	if keys < 1 {
		keys = 10000
	}

	for i := 0; i < keys; i++ {
		bs := randBytes(128 + mrand.Intn(256))
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			return nil, errors.New(rs.Message)
		}
	}

	dbTestSample = dbs[0]
	return dbTestSample, nil
}

func Test_Object_Common(t *testing.T) {

	round := 0

	for pn, ports := range [][]int{
		{},                    // local embedded
		{10001, 10002, 10003}, // cluster
	} {

		dbs, err := dbTestOpen(ports, false)
		if err != nil {
			t.Fatalf("Can Not Open Database %s", err.Error())
		}

		dbt := []*Conn{dbs[0]}

		if len(ports) > 1 {
			if dbs2, err := dbTestOpen(ports, true); err != nil {
				t.Fatalf("Can Not Open Database %s", err.Error())
			} else {
				dbt = append(dbt, dbs2[0])
			}
		}

		for _, db := range dbt {

			round++
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
					} else if rs.LogVersion < 100 {
						t.Fatal("Query Key ER! LogVersion")
					} else {
						t.Logf("Query Key OK, LogVersion %d", rs.LogVersion)
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
				logID := uint64(0)
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version < 1 {
					t.Fatal("Object LogId ER!")
				} else {
					logID = rs.Meta.Version
				}

				if rs := db.NewWriter(key, 123).Commit(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				}
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version != logID {
					t.Fatal("Object LogId ER!")
				}

				if rs := db.NewWriter(key, 321).Commit(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				}
				if rs := db.NewReader(key).Query(); !rs.OK() {
					t.Fatal("Object LogId ER!")
				} else if rs.Meta.Version <= logID {
					t.Fatal("Object LogId ER!")
				} else {
					t.Logf("Object LogId OK from %d to %d", logID, rs.Meta.Version)
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

				req := db.NewReader(nil).
					KeyRangeSet([]byte("0001"), []byte("0009")).
					LimitNumSet(10)
				req.Attrs |= kv2.ObjectMetaAttrDataOff
				if rs := req.Query(); !rs.OK() {
					t.Fatal("Query ER!")
				} else {

					if len(rs.Items) != 2 {
						t.Fatalf("Query KeyRange ER! %d", len(rs.Items))
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
				if rs := db.NewReader(nil).KeyRangeSet([]byte("0000"), []byte("0001z")).Query(); !rs.OK() {
					t.Fatal("Query Key ER! Expired")
				}
				time.Sleep(1e9)
				if rs := db.NewReader([]byte("0001")).Query(); !rs.NotFound() {
					t.Fatalf("Query Key ER! Expired")
				} else {
					t.Logf("Commit Expirted OK")
				}
				if rs := db.NewReader(nil).KeyRangeSet([]byte("0000"), []byte("0001z")).Query(); !rs.NotFound() {
					t.Fatalf("Query Key ER! Expired %v", rs)
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

			// Delete Data Only
			{
				key := []byte(fmt.Sprintf("delete-data-only-%d", round))
				wr := kv2.NewObjectWriter(key, "demo")
				if rs := db.Commit(wr); rs.OK() {
					t.Log("Delete DataOnly Step-1 OK")

				} else {
					t.Fatal("Delete DataOnly Step-1 ER!")
				}

				if mrand.Intn(2) == 0 {
					batch := kv2.NewBatchRequest("")
					e := batch.Delete(key)
					e.Mode |= kv2.ObjectWriterModeDeleteDataOnly
					if rs := db.BatchCommit(batch); rs.OK() {
						t.Log("Delete DataOnly Step-2 OK")
					} else {
						t.Fatal("Delete DataOnly Step-2 ER!")
					}
				} else {
					wr = kv2.NewObjectWriter(key, "demo").ModeDeleteSet(true)
					wr.Mode |= kv2.ObjectWriterModeDeleteDataOnly
					if rs := db.Commit(wr); rs.OK() {
						t.Log("Delete DataOnly Step-2 OK")
					} else {
						t.Fatal("Delete DataOnly Step-2 ER!")
					}
				}

				rr := db.NewReader(key)
				if rs := rr.Query(); rs.NotFound() {
					t.Log("Delete DataOnly Step-3 OK")
				} else {
					t.Fatal("Delete DataOnly Step-3 ER!")
				}

				rr = db.NewReader(key)
				rr.Mode |= kv2.ObjectReaderModeMetaOnly
				if rs := rr.Query(); rs.OK() {
					t.Logf("Delete DataOnly Step-4 OK")
				} else {
					t.Fatal("Delete DataOnly Step-4 ER!")
				}
			}

			// Meta Extra
			{
				extra := []byte("extra-data")

				key := []byte("meta-extra-data")
				wr := kv2.NewObjectWriter(key, "demo")
				wr.ExtraSet(extra)
				if rs := db.Commit(wr); rs.OK() {
					t.Log("Meta Extra Put OK")
				} else {
					t.Fatal("Meta Extra Put ER!")
				}

				rr := db.NewReader(key)
				if rs := rr.Query(); rs.OK() && bytes.Compare(extra, rs.Meta.Extra) == 0 {
					t.Log("Meta Extra Get OK")
				} else {
					t.Fatal("Meta Extra Get ER!")
				}

				rr = db.NewReader(key)
				rr.Mode |= kv2.ObjectReaderModeMetaOnly
				if rs := rr.Query(); rs.OK() && bytes.Compare(extra, rs.Meta.Extra) == 0 {
					t.Log("Meta Extra Get OK")
				} else {
					t.Fatal("Meta Extra Get ER!")
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

	dbs, err := dbTestOpen([]int{20001, 20002, 20003}, false)
	if err != nil {
		t.Fatalf("Can Not Open Database %s", err.Error())
	}

	var (
		key   = "log-async-key"
		value = "log-async-test"
		extra = []byte("extra-data")
		attrs = [][]uint64{
			{0, 0},
			{kv2.ObjectMetaAttrMetaOff, 0},
			{kv2.ObjectMetaAttrDataOff, 0},
		}
	)

	for i, va := range attrs {
		ow := kv2.NewObjectWriter([]byte(fmt.Sprintf("%s-%d", key, va[0])), value)
		ow.Meta.Attrs |= va[0]
		ow.ExtraSet(extra)
		if rs := dbs[0].commitLocal(ow, 0); !rs.OK() {
			t.Fatalf("Commit ER! %s", rs.Message)
		} else {
			attrs[i][1] = rs.Meta.Version
			t.Logf("Commit OK cLog %d", attrs[i][1])
		}
	}

	time.Sleep(1100e6)

	ctx, fc := context.WithTimeout(context.Background(), time.Second*1)
	defer fc()

	for _, db := range dbs {

		for _, hp := range db.opts.Cluster.MainNodes {

			conn, err := clientConn(hp.Addr, hp.AccessKey, hp.AuthTLSCert, false)
			if err != nil {
				t.Fatalf("Object AsyncLog ER %s, node %s", err.Error(), hp.Addr)
			}

			for _, va := range attrs {

				rr := kv2.NewObjectReader([]byte(fmt.Sprintf("%s-%d", key, va[0])))
				rr.Attrs |= va[0]

				rs, err := kv2.NewPublicClient(conn).Query(ctx, rr)
				if err != nil {
					t.Fatalf("Object AsyncLog ER %s, node %s", err.Error(), hp.Addr)
				}

				if !rs.OK() || len(rs.Items) == 0 || bytes.Compare(extra, rs.Items[0].Meta.Extra) != 0 {
					t.Fatalf("Object AsyncLog ER, ok %v, msg %s, len %d, node %s",
						rs.OK(), rs.Message, len(rs.Items), hp.Addr)
				} else {
					t.Logf("Object AsyncLog OK, node %s", hp.Addr)
				}

				if rs.Items[0].Meta.Version != va[1] {
					t.Fatalf("Object AsyncLog ER %d/%d", rs.Items[0].Meta.Version, va[1])
				}

				if kv2.AttrAllow(va[0], kv2.ObjectMetaAttrDataOff) {

					rr2 := kv2.NewObjectReader([]byte(fmt.Sprintf("%s-%d", key, va[0])))
					rr2.Attrs |= kv2.ObjectMetaAttrMetaOff

					if rs, err := kv2.NewPublicClient(conn).Query(ctx, rr2); err == nil && rs.OK() {
						t.Fatalf("Object AsyncLog ER %d/%d", rs.Items[0].Meta.Version, va[1])
					}
				}

				if kv2.AttrAllow(va[0], kv2.ObjectMetaAttrMetaOff) {

					rr2 := kv2.NewObjectReader([]byte(fmt.Sprintf("%s-%d", key, va[0])))
					rr2.Attrs |= kv2.ObjectMetaAttrDataOff

					if rs, err := kv2.NewPublicClient(conn).Query(ctx, rr2); err == nil && rs.OK() {
						t.Fatalf("Object AsyncLog ER %d/%d", rs.Items[0].Meta.Version, va[1])
					}
				}
			}

			// t.Logf("Object AsyncLog Bind %s, Node %s OK", db.opts.Server.Bind, hp)
		}

		t.Logf("Object AsyncLog Bind %s, MainNodes %d OK",
			db.opts.Server.Bind, len(db.opts.Cluster.MainNodes))
	}
}

func Test_Table(t *testing.T) {

	dbs, err := dbTestOpen([]int{}, false)
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

	dbs, err := dbTestOpen(nil, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := dataSample()
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Seq_MetaLogDisable(b *testing.B) {

	dbs, err := dbTestOpen([]int{-1}, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := dataSample()
		if rs := dbs[0].NewWriter([]byte(fmt.Sprintf("%032d", i)), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand(b *testing.B) {

	dbs, err := dbTestOpen(nil, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := dataSample()
		if rs := dbs[0].NewWriter(
			[]byte(fmt.Sprintf("%032d", mrand.Int31())), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand_MetaLogDisable(b *testing.B) {

	dbs, err := dbTestOpen([]int{-2}, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := dataSample()
		if rs := dbs[0].NewWriter(
			[]byte(fmt.Sprintf("%032d", mrand.Int31())), bs).Commit(); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand_Cluster_x3_1000(b *testing.B) {

	dbs, err := dbTestOpen([]int{10001, 10002, 10003}, false)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := randBytes(mrand.Intn(2000))
		ow := kv2.NewObjectWriter(
			[]byte(fmt.Sprintf("%032d", mrand.Int31())), bs)
		if rs := dbs[mrand.Intn(len(dbs))].Commit(ow); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Commit_Rand_Cluster_x3_100(b *testing.B) {

	dbs, err := dbTestOpen([]int{10001, 10002, 10003}, false)

	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		bs := randBytes(mrand.Intn(200))
		ow := kv2.NewObjectWriter(
			[]byte(fmt.Sprintf("%032d", mrand.Int31())), bs)
		if rs := dbs[mrand.Intn(len(dbs))].Commit(ow); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Query_Key(b *testing.B) {

	db, err := dbSample(10000)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		if rs := db.NewReader([]byte(fmt.Sprintf("%032d", mrand.Intn(10000)))).Query(); !rs.OK() {
			b.Fatalf("Query ER!, Err %s", rs.Message)
		}
	}
}

func Benchmark_Query_KeyRange(b *testing.B) {

	db, err := dbSample(10000)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}

	for i := 0; i < b.N; i++ {
		offset := mrand.Intn(10000 - 10)
		if rs := db.NewReader(nil).KeyRangeSet(
			[]byte(fmt.Sprintf("%032d", offset)),
			[]byte(fmt.Sprintf("%032d", offset+10))).
			LimitNumSet(10).Query(); !rs.OK() {
			b.Fatalf("Query KeyRange ER!, Err %s", rs.Message)
		}
	}
}
