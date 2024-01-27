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
	"strings"
	"testing"
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
)

func Test_DatabaseReplica_Task(t *testing.T) {

	// reset global params for test only
	{
		testLocalMode = true

		kShardSplit_CapacitySize_Def = 32
		kShardSplit_CapacitySize_Min = 32

		kReplicaRebalance_StoreCapacityThreshold = 0.01
	}

	const (
		test_DatabaseReplica_Task = "test_db_replica_task"
	)

	//
	sess, err := test_ServiceApi_RepX_Open("replica_task", "compression_with_none")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.release()

	{
		if rs := sess.ac.DatabaseCreate(&kvapi.DatabaseCreateRequest{
			Name:       test_DatabaseReplica_Task,
			Engine:     storage.DefaultDriver,
			ReplicaNum: 3,
		}); !rs.OK() {
			t.Fatal(rs.StatusMessage)
		} else {
			t.Logf("database create ok, meta %v", rs.Meta())
		}
	}

	sess.c.SetDatabase(test_DatabaseReplica_Task)

	statusRefresh := func(r int) {
		// force update status
		sess.db.taskReplicaListRefresh(true)

		// check db size
		var tm = sess.db.dbMapMgr.getByName(test_DatabaseReplica_Task)
		if tm == nil {
			t.Fatalf("setup database fail")
		}

		tn := timesec()

		tm.iterStatusDisplay(func(shard *kvapi.DatabaseMap_Shard, shardStatus string, repStatus []string) {
			t.Logf("#%d, shard % 4d %s %ds, replica status [%s], key %v",
				r, shard.Id, shardStatus, tn-shard.Updated,
				strings.Join(repStatus, "] ["), shard.LowerKey)
		})

		sess.db.jobReplicaRebalanceSetup(true)
	}

	go func() {
		for {
			time.Sleep(1e6)
			var (
				key = fmt.Sprintf("key-%02d-%08d", 1, mrand.Uint32()%100000)
				val = randBytes(1 << 10)
			)
			if rs := sess.c.Write(kvapi.NewWriteRequest([]byte(key), []byte(val))); !rs.OK() {
				t.Fatalf("Write ER!, Err %s", rs.ErrorMessage())
			} else {
				// t.Logf("Write OK, Log %d", rs.Meta().Version)
			}
		}
	}()

	for r := 1; r <= 20; r++ {
		t.Logf("Round %d", r)
		// Write
		if r <= 2 {
			for i := 0; i < 100; i++ {
				var (
					key = fmt.Sprintf("key-%02d-%08d", r, i)
					val = randBytes(1 << 19)
				)
				if rs := sess.c.Write(kvapi.NewWriteRequest([]byte(key), []byte(val))); !rs.OK() {
					t.Fatalf("Write ER!, Err %s", rs.ErrorMessage())
				} else {
					// t.Logf("Write OK, Log %d", rs.Meta().Version)
				}
			}
		}

		if false {
			// force update status
			sess.db.taskReplicaListRefresh(true)

			// check db size
			var tm = sess.db.dbMapMgr.getByName(test_DatabaseReplica_Task)
			if tm == nil {
				t.Fatalf("setup database fail")
			}

			var shards = tm.lookupByRange([]byte{}, []byte{0xff}, false)
			if len(shards) == 0 || len(shards[0].replicas) != 3 {
				t.Fatalf("setup shards/replicas fail")
			}

			for _, shard := range shards {
				for _, rep := range shard.replicas {
					t.Logf("shard %d replica %d size %d", shard.shardId, rep.replicaId, rep.status.storageUsed.value)
				}
			}
		}

		{
			sess.db.taskReplicaListRefresh(true)
			sess.db.jobShardSplitSetup()

			sess.db.jobKeyMapSetup()
		}

		statusRefresh(0)

		time.Sleep(1e9)
	}

	for r := 1; r <= 1000; r++ {

		statusRefresh(r)

		if false && r >= 10 && r <= 20 {
			for i := 0; i < 50; i++ {
				var (
					key = fmt.Sprintf("key-%02d-%08d", 1, mrand.Uint32()%10000)
					val = randBytes(1 << 19)
				)
				if rs := sess.c.Write(kvapi.NewWriteRequest([]byte(key), []byte(val))); !rs.OK() {
					t.Fatalf("Write ER!, Err %s", rs.ErrorMessage())
				} else {
					// t.Logf("Write OK, Log %d", rs.Meta().Version)
				}
			}
		}

		time.Sleep(1e9)
	}
}
