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
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

func (it *dbServer) taskRun() {
	tr := time.NewTimer(1e9)
	for !it.close {
		select {
		case <-tr.C:
			if err := it.taskRefresh(); err != nil {
				hlog.Printf("warn", "task refresh err %s", err.Error())
			}
			tr.Reset(1e9)
		}
	}
}

func (it *dbServer) taskRefresh() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if err := it.taskReplicaListRefresh(false); err != nil {
		return err
	}
	return nil
}

func (it *dbServer) taskReplicaListRefresh(forceRefresh bool) error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	it.tableMapMgr.iter(func(tm *tableMap) {

		//
		if tm.meta == nil || tm.data == nil || tm.data.ReplicaNum < 1 ||
			tm.mapMeta == nil || tm.mapData == nil {
			return
		}

		//
		it._task_statusRefresh(tm, tm.mapData, forceRefresh)

		//
		it._task_replicaRemove(tm, tm.mapData)
	})

	return nil
}

func (it *dbServer) _task_statusRefresh(tm *tableMap, mapData *kvapi.TableMap, forceRefresh bool) {

	for i := 0; i < len(mapData.Shards); i++ {

		var (
			shard    = mapData.Shards[i]
			lowerKey = bytesClone(shard.LowerKey)
			upperKey []byte
		)

		if i+1 < len(mapData.Shards) {
			upperKey = bytesClone(mapData.Shards[i+1].LowerKey)
		} else {
			upperKey = []byte{0xff}
		}

		for _, rep := range shard.Replicas {
			repInst := tm.tryInitReplica(shard, rep)
			if repInst == nil {
				continue
			}
			repInst.taskStatusRefresh(shard, lowerKey, upperKey, forceRefresh)
		}
	}
}

func (it *dbServer) _task_replicaRemove(tm *tableMap, mapData *kvapi.TableMap) {

	for i := 0; i < len(mapData.Shards); i++ {

		var shard = mapData.Shards[i]

		if len(shard.Replicas) <= int(tm.data.ReplicaNum) {
			continue
		}

		var (
			lowerKey = bytesClone(shard.LowerKey)
			upperKey []byte
		)

		if i+1 < len(mapData.Shards) {
			upperKey = bytesClone(mapData.Shards[i+1].LowerKey)
		} else {
			upperKey = []byte{0xff}
		}

		for _, rep := range shard.Replicas {
			//
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
				continue
			}

			//
			repInst := tm.tryInitReplica(shard, rep)
			if repInst == nil {
				continue
			}

			if kvapi.AttrAllow(repInst.status.action, kReplicaStatus_Remove) {
				continue
			}

			if err := repInst._task_deleteRange(it, shard, lowerKey, upperKey); err == nil {
				repInst.status.action = kReplicaStatus_Remove
				tm.tryRemoveReplica(rep.Id)
			}
		}
	}
}
