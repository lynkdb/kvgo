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

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
	_ "github.com/lynkdb/kvgo/pkg/storage/pebble"
)

func Test_TableJob_LogPull(t *testing.T) {

	const (
		test_TableJob_LogPull_Table = "test_tablejob_logpull"
	)

	sess, err := test_ServiceApi_RepX_Open("logpull")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		if rs := sess.ac.TableCreate(&kvapi.TableCreateRequest{
			Name:       test_TableJob_LogPull_Table,
			Engine:     storage.DefaultDriver,
			ReplicaNum: 2,
		}); !rs.OK() {
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

	sess.c.SetTable(test_TableJob_LogPull_Table)

	var tm = sess.db.tableMapMgr.getByName(test_TableJob_LogPull_Table)
	if tm == nil {
		t.Fatalf("setup table fail")
	}

	var shards = tm.lookupByRange([]byte{}, []byte{0xff}, false)
	if len(shards) != 1 || len(shards[0].replicas) != 2 {
		t.Fatalf("setup shards/replicas fail")
	}

	replicaClients := []*tableReplica{}
	for _, rep := range shards[0].replicas {
		replicaClients = append(replicaClients, rep)
	}
	t.Logf("replicaClients %d", len(replicaClients))

	for r := 0; r < 2; r++ {

		var (
			testNum = 100 + mrand.Intn(1000)
		)

		//
		for id := 0; id < testNum; id++ {
			replicaClients[0].Write(kvapi.NewWriteRequest(
				[]byte(fmt.Sprintf("r%d-key-%08d", r, id)),
				[]byte(fmt.Sprintf("r%d-value-%d", r, id))))
		}
		t.Logf("init keys %d", testNum)

		type testCase struct {
			LowerKey []byte
			UpperKey []byte
			Num      int
		}

		sess.db.jobTableLogPull(true)

		// Log, TTL test
		for i, c1 := range replicaClients {
			if i == 0 {
				continue
			}

			for _, reqCase := range []*testCase{
				{
					LowerKey: keyLogEncode(c1.replicaId, 0),
					UpperKey: keyLogEncode(c1.replicaId, 100000),
					Num:      0,
				},
				{
					LowerKey: keyEncode(nsKeyMeta, []byte(fmt.Sprintf("r%d-key-", r))),
					UpperKey: keyEncode(nsKeyMeta, []byte(fmt.Sprintf("r%d-key-9", r))),
					Num:      testNum,
				},
				{
					LowerKey: keyEncode(nsKeyData, []byte(fmt.Sprintf("r%d-key-", r))),
					UpperKey: keyEncode(nsKeyData, []byte(fmt.Sprintf("r%d-key-9", r))),
					Num:      testNum,
				},
			} {

				req := &kvapi.RangeRequest{
					LowerKey: reqCase.LowerKey,
					UpperKey: reqCase.UpperKey,
					Limit:    10 + int64(mrand.Intn(100)),
				}

				num := 0

				for {
					items, err := c1.RawRange(req)
					if err != nil {
						t.Fatal(err)
					}
					num += len(items)
					for _, item := range items {
						req.LowerKey = item.Key
					}
					if len(items) < 10 {
						break
					}
				}

				if num != reqCase.Num {
					t.Fatalf("replica %d, num diff %d - %d", c1.replicaId, reqCase.Num, num)
				} else {
					t.Logf("replica %d, num hit %d - %d", c1.replicaId, reqCase.Num, num)
				}
			}
		}
	}
}
