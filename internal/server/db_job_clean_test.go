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
	mrand "math/rand"
	"testing"
	"time"

	"github.com/lynkdb/lynkapi/go/lynkapi"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
)

func Test_DatabaseJob_Clean(t *testing.T) {

	const (
		test_DatabaseJob_Clean_Database = "test_dbjob_clean"
	)

	sess, err := test_ServiceApi_RepX_Open("clean")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		req, _ := lynkapi.NewRequestFromObject("AdminService", "DatabaseCreate", &kvapi.DatabaseCreateRequest{
			Name:       test_DatabaseJob_Clean_Database,
			Engine:     storage.DefaultDriver,
			ReplicaNum: 3,
		})

		if rs := sess.ac.Exec(req); !rs.OK() {
			t.Fatal(rs.Err())
		} else {
			t.Logf("database create ok, meta %v", *rs.Data)
		}

		req, _ = lynkapi.NewRequestFromObject("AdminService", "DatabaseList", &kvapi.DatabaseListRequest{})
		if rs := sess.ac.Exec(req); !rs.OK() {
			t.Fatal(rs.Err())
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

	sess.c = sess.c.SetDatabase(test_DatabaseJob_Clean_Database)

	tm := sess.db.dbMapMgr.getByName(test_DatabaseJob_Clean_Database)
	if tm == nil {
		t.Fatalf("setup database fail")
	}

	var shards = tm.lookupByRange([]byte{}, []byte{0xff}, false)
	if len(shards) != 1 || len(shards[0].replicas) != 3 {
		t.Fatalf("setup shards/replicas fail")
	}

	replicaClients := []*dbReplica{}
	for _, rep := range shards[0].replicas {
		replicaClients = append(replicaClients, rep)
	}
	t.Logf("replicaClients %d", len(replicaClients))

	var (
		testNum = 100 + mrand.Intn(1000)
		tn      = time.Now().UnixNano() / 1e6
	)

	//
	for id := 0; id < testNum; id++ {
		rs := sess.c.Write(kvapi.NewWriteRequest(
			[]byte(fmt.Sprintf("key-%08d", id)),
			[]byte(fmt.Sprintf("value-%d", id))).SetTTL(1000),
		)
		if !rs.OK() {
			t.Fatal(rs.StatusMessage)
		}
	}

	tto := (time.Now().UnixNano() / 1e6) + 1000

	// Log, TTL test
	for _, c1 := range replicaClients {

		for _, req := range []*kvapi.RangeRequest{
			&kvapi.RangeRequest{
				LowerKey: keyExpireEncode(0, nil),
				UpperKey: keyExpireEncode(tto, nil),
				Limit:    10 + int64(mrand.Intn(100)),
			},
			&kvapi.RangeRequest{
				LowerKey: keyLogEncode(0, 0, 0),
				UpperKey: keyLogEncode(100000, 0, 0),
				Limit:    10 + int64(mrand.Intn(100)),
			},
		} {
			num := 0

			for {
				items, err := c1.RawRange(req)
				if err != nil {
					t.Fatal(err)
				}
				num += len(items)
				for _, item := range items {
					meta, err := kvapi.LogDecode(item.Value)
					if err != nil {
						t.Fatal(err)
					}
					if len(meta.Key) != 12 {
						t.Fatalf("err key %s", string(meta.Key))
					}
					if meta.Expired < tn || meta.Expired > tto {
						t.Fatalf("expired time err")
					}
					req.LowerKey = item.Key
				}
				if len(items) < 10 {
					break
				}
			}

			if num != testNum {
				t.Fatalf("replica %d, num diff %d - %d", c1.replicaId, testNum, num)
			} else {
				t.Logf("replica %d, num hit %d - %d", c1.replicaId, testNum, num)
			}
		}

		for _, req := range []*kvapi.RangeRequest{
			&kvapi.RangeRequest{
				LowerKey: keyEncode(nsKeyMeta, []byte("key-")),
				UpperKey: keyEncode(nsKeyMeta, []byte("key-9")),
				Limit:    10 + int64(mrand.Intn(100)),
			},
			&kvapi.RangeRequest{
				LowerKey: keyEncode(nsKeyData, []byte("key-")),
				UpperKey: keyEncode(nsKeyData, []byte("key-9")),
				Limit:    10 + int64(mrand.Intn(100)),
			},
		} {
			num := 0

			for {
				items, err := c1.RawRange(req)
				if err != nil {
					t.Fatal(err)
				}
				num += len(items)
				for _, item := range items {
					meta, _, err := kvapi.MetaDecode(item.Value)
					if err != nil {
						t.Fatal(err)
					}
					if meta.Expired < tn || meta.Expired > tto {
						t.Fatalf("expired time err")
					}
					req.LowerKey = item.Key
				}
				if len(items) < 10 {
					break
				}
			}

			if num != testNum {
				t.Fatalf("replica %d, num diff %d - %d", c1.replicaId, testNum, num)
			} else {
				t.Logf("replica %d, num hit %d - %d", c1.replicaId, testNum, num)
			}
		}
	}

	time.Sleep(1e9)
	for _, c1 := range replicaClients {
		c1._jobCleanTTL()
		for _, req := range []*kvapi.RangeRequest{
			&kvapi.RangeRequest{
				LowerKey: keyExpireEncode(0, nil),
				UpperKey: keyExpireEncode(tto, nil),
				Limit:    10,
			},
			&kvapi.RangeRequest{
				LowerKey: keyEncode(nsKeyMeta, []byte("key-")),
				UpperKey: keyEncode(nsKeyMeta, []byte("key-9")),
				Limit:    10,
			},
			&kvapi.RangeRequest{
				LowerKey: keyEncode(nsKeyData, []byte("key-")),
				UpperKey: keyEncode(nsKeyData, []byte("key-9")),
				Limit:    10,
			},
		} {

			items, err := c1.RawRange(req)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != 0 {
				t.Fatalf("num diff %d", len(items))
			} else {
				t.Logf("clean check ok")
			}
		}
	}
}
