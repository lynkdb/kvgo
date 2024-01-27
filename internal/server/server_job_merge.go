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
	"sort"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

const jobMergeName = "job-merge"

func (it *dbServer) jobShardMergeSetup(force bool) error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		tn = timesec()
		vs *storeStats
	)

	it.storeMgr.mergePending = 0

	mergeCheck := func(tm *dbMap, mapData *kvapi.DatabaseMap) bool {

		if len(mapData.Jobs) == 0 {
			return false
		}

		var (
			chg            = false
			nextMapVersion = mapData.Version + 1
			jobIndex       = -1
			mergeOut       *kvapi.DatabaseMap_Shard
			mergeIn        *kvapi.DatabaseMap_Shard
		)

		for i, job := range mapData.Jobs {
			if job.Type == kvapi.DatabaseMap_Merge && len(job.ShardIds) == 2 {
				mergeOut = mapData.GetShard(job.ShardIds[0])
				mergeIn = mapData.GetShard(job.ShardIds[1])
				jobIndex = i
				break
			}
		}

		if mergeOut == nil || mergeIn == nil {
			return false
		}

		it.storeMgr.mergePending += 1

		var (
			inReady = 0
			out     = 0
		)

		// move-in
		for _, rep := range mergeOut.Replicas {

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				continue
			}

			if repInst.localStatus.action.mapVersion != mergeOut.Version ||
				!kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				continue
			}

			if kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveIn) {
				rep.Action, chg = kReplicaSetup_In, true
				it.auditLogger.Put("merge", "shard-out %d, rep %d, move-in done", mergeOut.Id, rep.Id)
			}

			if kvapi.AttrAllow(rep.Action, kReplicaSetup_In) &&
				!kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
				inReady += 1
			}
		}

		if inReady < int(tm.data.ReplicaNum) {
			return false
		}

		if chg {
			mapData.Version, mergeOut.Version = nextMapVersion, nextMapVersion
			mergeOut.Updated = timesec()
			it.auditLogger.Put("merge", "shard-out %d, move-in done", mergeOut.Id)
			return chg
		}

		{
			// move-out
			for _, rep := range mergeOut.Replicas {

				if !kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
					continue
				}

				out += 1

				rep.Action, chg = kReplicaSetup_Out|kReplicaSetup_Remove, true

				it.auditLogger.Put("merge", "shard-out %d, rep %d, move-out done",
					mergeOut.Id, rep.Id)
			}

			if out > 0 {
				sort.Slice(mergeOut.Replicas, func(i, j int) bool {
					return mergeOut.Replicas[i].Action < mergeOut.Replicas[j].Action
				})
			}
		}

		if inReady+out != len(mergeOut.Replicas) {
			return false
		}

		var (
			replicaInSet  = map[uint64]bool{}
			replicaOutHit = 0
		)

		for _, rep := range mergeIn.Replicas {
			//
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
				continue
			}

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				continue
			}

			if !kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				continue
			}

			replicaInSet[rep.StoreId] = true
		}

		for _, rep := range mergeOut.Replicas {
			//
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
				continue
			}

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				continue
			}

			if !kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				continue
			}

			if _, ok := replicaInSet[rep.StoreId]; ok {
				replicaOutHit += 1
			}
		}

		if replicaOutHit == len(mergeIn.Replicas) {

			mergeOut.Action, mergeOut.Version, mergeOut.Updated = (kShardSetup_Out | kShardSetup_MergeOut), nextMapVersion, tn

			mergeIn.Action, mergeIn.Version, mergeIn.Updated = kvapi.AttrRemove(mergeIn.Action, kShardSetup_MergeIn), nextMapVersion, tn

			mapData.Version = nextMapVersion

			mapData.Jobs = append(mapData.Jobs[:jobIndex], mapData.Jobs[jobIndex+1:]...)

			chg = true

			it.storeMgr.mergePending -= 1
			it.auditLogger.Put("merge", "shard-out %d, shard-in %d, done", mergeOut.Id, mergeIn.Id)
			hlog.Printf("info", "shard-out %d, shard-in %d, done", mergeOut.Id, mergeIn.Id)
		}

		return chg
	}

	type mergeEntry struct {
		prev *kvapi.DatabaseMap_Shard
		curr *kvapi.DatabaseMap_Shard
		size int64
	}

	mergeSchedule := func(tm *dbMap, mapData *kvapi.DatabaseMap) bool {

		if len(mapData.Jobs) > 0 {
			return false
		}

		var (
			freeSize       = tm.shardSize() * 2
			mergeSize      = tm.shardSize() / 3
			nextMapVersion = mapData.Version + 1
		)

		// // shards
		// for _, shard := range mapData.Shards {

		// 	if shard.Action != kShardSetup_In {
		// 		return false
		// 	}

		// 	ss := tm.shardStatus(shard, shard.Version, kReplicaSetup_In, kReplicaStatus_Ready)
		// 	if len(ss.replicas) < int(tm.data.ReplicaNum) {
		// 		return false
		// 	}
		// }

		if vs == nil || len(vs.Items) < 2 {
			return false
		}

		sort.Slice(vs.Items, func(i, j int) bool {
			return vs.Items[i].Free < vs.Items[j].Free
		})

		var (
			hotStore  = vs.Items[0]
			coldStore = vs.Items[len(vs.Items)-1]
			diffAbs   = absInt64(hotStore.Free - coldStore.Free)
			merges    []*mergeEntry
		)

		if diffAbs < freeSize {
			hlog.Printf("debug", "db %s, skip free-size %d MiB",
				tm.data.Name, diffAbs)
			// return false
		}

		dbMapShardIter(mapData, func(prev, curr, next *kvapi.DatabaseMap_Shard) {

			if prev == nil {
				return
			}

			if prev.Action != kShardSetup_In ||
				curr.Action != kShardSetup_In {
				return
			}

			if prev.Updated+kShardMerge_JobIntervalSeconds > tn ||
				curr.Updated+kShardMerge_JobIntervalSeconds > tn {
				return
			}

			if len(prev.Replicas) > int(tm.data.ReplicaNum) ||
				len(curr.Replicas) > int(tm.data.ReplicaNum) {
				return
			}

			ssCurr := tm.shardStatus(curr, curr.Version, kReplicaSetup_In, kReplicaStatus_Ready)
			if len(ssCurr.replicas) < int(tm.data.ReplicaNum) {
				return
			}

			if ssCurr.avgSize > mergeSize {
				return
			}

			merges = append(merges, &mergeEntry{
				prev: prev,
				curr: curr,
				size: ssCurr.avgSize,
			})
		})

		if len(merges) == 0 {
			return false
		}

		sort.Slice(merges, func(i, j int) bool {
			return merges[i].size < merges[j].size
		})

		var (
			merge     = merges[0]
			stores    = map[uint64]bool{}
			srcReps   = []*kvapi.DatabaseMap_Replica{}
			dstStores = []uint64{}
		)

		for _, rep := range merge.prev.Replicas {
			if kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
				stores[rep.StoreId] = true
			}
		}

		if len(stores) < int(tm.data.ReplicaNum) {
			return false
		}

		for _, rep := range merge.curr.Replicas {
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_In) {
				continue
			}
			if _, ok := stores[rep.StoreId]; ok {
				delete(stores, rep.StoreId)
			} else {
				srcReps = append(srcReps, rep)
			}
		}

		if len(srcReps) != len(stores) {
			return false
		}

		for id, _ := range stores {
			dstStores = append(dstStores, id)
		}

		job := &kvapi.DatabaseMap_JobDescriptor{
			Type:         kvapi.DatabaseMap_Merge,
			ShardIds:     []uint64{merge.curr.Id, merge.prev.Id},
			ReplicaMoves: map[uint64]uint64{},
		}

		for i, srcReplica := range srcReps {
			srcReplica.Action |= kReplicaSetup_MoveOut

			dstReplica := &kvapi.DatabaseMap_Replica{
				Id:      tm.nextIncr(),
				Prev:    srcReplica.Id,
				StoreId: dstStores[i],
				Action:  kReplicaSetup_MoveIn,
			}

			job.ReplicaMoves[srcReplica.Id] = dstReplica.Id

			merge.curr.Replicas = append(merge.curr.Replicas, dstReplica)
		}

		merge.curr.Action |= kShardSetup_MergeOut
		merge.curr.Version = nextMapVersion
		merge.curr.Updated = timesec()

		merge.prev.Action |= kShardSetup_MergeIn
		merge.prev.Version = nextMapVersion
		merge.prev.Updated = merge.curr.Updated

		mapData.Jobs = append(mapData.Jobs, job)

		mapData.Version = nextMapVersion
		mapData.Updated = merge.curr.Updated

		it.storeMgr.lastMergeUpdated = mapData.Updated
		it.storeMgr.mergePending += 1

		it.auditLogger.Put("merge", "shard %d, to %d, replica moves %d, start",
			merge.curr.Id, merge.prev.Id, len(srcReps))

		hlog.Printf("info", "shard %d, to %d, replica moves %d, start",
			merge.curr.Id, merge.prev.Id, len(srcReps))

		return true
	}

	dbAction := func(tm *dbMap) {

		it.jobSetupMut.Lock()
		defer it.jobSetupMut.Unlock()

		if tm.mapData.Id == sysDatabaseId {
			return
		}

		var (
			prevMetaVersion = tm.mapMeta.Version
			mapData         kvapi.DatabaseMap
		)

		if err := objectClone(tm.mapData, &mapData); err != nil {
			return
		}

		if chg := mergeCheck(tm, &mapData); chg {
			if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			}
			return
		}

		if it.storeMgr.mergePending > 0 {
			return
		}

		if mapData.Updated+kShardMerge_JobIntervalSeconds > tn {
			return
		}

		if vs == nil {
			vs = it.storeMgr.stats(jobMergeName)
		}

		if chg := mergeSchedule(tm, &mapData); chg {
			if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			}
			return
		}
	}

	it.dbMapMgr.iter(dbAction)

	return nil
}
