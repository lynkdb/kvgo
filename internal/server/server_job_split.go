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
	"sort"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

type _jobSplitReplicaItem struct {
	rep  *dbReplica
	size int64
}

type _jobSplitShardItem struct {
	shard    *kvapi.DatabaseMap_Shard
	next     *kvapi.DatabaseMap_Shard
	sizeAvg  int64
	replicas []*_jobSplitReplicaItem
}

func (it *dbServer) jobShardSplitSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		tn           = timesec()
		splitPending = false
	)

	splitInCheck := func(tm *dbMap, shard *kvapi.DatabaseMap_Shard, nextMapVersion uint64) bool {

		if !kvapi.AttrAllow(shard.Action, kShardSetup_SplitIn) {
			return false
		}

		splitPending = true

		if len(shard.Replicas) < 1 || len(shard.Replicas) < int(tm.data.ReplicaNum) {
			return false
		}

		for _, rep := range shard.Replicas {

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				return false
			}

			if repInst.localStatus.action.mapVersion != shard.Version ||
				!kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				return false
			}
		}

		shard.Action = kvapi.AttrRemove(shard.Action, kShardSetup_SplitIn)
		shard.Version = nextMapVersion
		shard.Updated = timesec()

		it.auditLogger.Put("split", "database %s, shard %d, split-in done", tm.data.Name, shard.Id)

		return true
	}

	splitOutCheck := func(tm *dbMap, shard *kvapi.DatabaseMap_Shard, nextMapVersion uint64) bool {

		if !kvapi.AttrAllow(shard.Action, kShardSetup_SplitOut) {
			return false
		}

		splitPending = true

		if len(shard.Replicas) < 1 || len(shard.Replicas) < int(tm.data.ReplicaNum) {
			return false
		}

		for _, rep := range shard.Replicas {

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				return false
			}

			if repInst.localStatus.action.mapVersion != shard.Version ||
				!kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
				return false
			}
		}

		shard.Action = kvapi.AttrRemove(shard.Action, kShardSetup_SplitOut)
		shard.Version = nextMapVersion
		shard.Updated = timesec()

		it.auditLogger.Put("split", "database %s, shard %d, split-out done", tm.data.Name, shard.Id)

		return true
	}

	splitCheck := func(tm *dbMap, mapData *kvapi.DatabaseMap) bool {

		var (
			chg            = false
			nextMapVersion = mapData.Version + 1
		)

		for _, shard := range mapData.Shards {

			if shard.Action == kShardSetup_In {
				continue
			}

			if splitInCheck(tm, shard, nextMapVersion) {
				chg = true
				break
			}

			if splitOutCheck(tm, shard, nextMapVersion) {
				chg = true
				break
			}
		}

		if chg {
			mapData.Version = nextMapVersion
			mapData.Updated = timesec()
		}

		return chg
	}

	splitSchedule := func(tm *dbMap, mapData *kvapi.DatabaseMap) *kvapi.DatabaseMap_Shard {

		var (
			splitSize = (3 * tm.shardSize()) / 2
			splitSets []*_jobSplitShardItem
		)

		for i, shard := range mapData.Shards {

			if len(shard.Replicas) != int(tm.data.ReplicaNum) {
				// testPrintf("shard %d %d %d", shard.Id, len(shard.Replicas), tm.data.ReplicaNum)
				continue
			}

			if shard.Action != kShardSetup_In {
				continue
			}

			if shard.Updated+kShardSplit_JobIntervalSeconds > tn {
				// testPrintf("shard %d split-in check skip time %d", shard.Id, tn-shard.Updated)
				continue
			}

			// testPrintf("shard %d split-in check time %d", shard.Id, tn-shard.Updated)

			si := &_jobSplitShardItem{
				shard: shard,
			}

			if i+1 < len(mapData.Shards) {
				si.next = mapData.Shards[i+1]
			}

			for _, rep := range shard.Replicas {

				repInst, ok := tm.replicas[rep.Id]
				if !ok {
					continue
				}

				if repInst.localStatus.storageUsed.mapVersion != shard.Version {
					break
				}

				si.sizeAvg += repInst.localStatus.storageUsed.value
				si.replicas = append(si.replicas, &_jobSplitReplicaItem{
					rep:  repInst,
					size: repInst.localStatus.storageUsed.value,
				})
			}

			if len(si.replicas) != len(shard.Replicas) {
				continue
			}

			si.sizeAvg = si.sizeAvg / int64(len(si.replicas))

			if si.sizeAvg <= splitSize {
				continue
			}

			splitSets = append(splitSets, si)
		}

		if len(splitSets) == 0 {
			return nil
		}

		sort.Slice(splitSets, func(i, j int) bool {
			return splitSets[i].sizeAvg > splitSets[j].sizeAvg
		})

		lowerShard := splitSets[0]
		sort.Slice(lowerShard.replicas, func(i, j int) bool {
			return lowerShard.replicas[i].size > lowerShard.replicas[j].size
		})

		mk, err := lowerShard.replicas[0].rep.trySplit(lowerShard.shard, lowerShard.next, lowerShard.sizeAvg)
		if err != nil {
			testPrintf("spit err %v", err)
			return nil
		}

		if bytes.Compare(mk, lowerShard.shard.LowerKey) <= 0 {
			return nil
		}

		mapData.Version += 1

		upperShard := &kvapi.DatabaseMap_Shard{
			Id:       tm.nextIncr(),
			Prev:     lowerShard.shard.Id,
			Version:  mapData.Version,
			LowerKey: mk,
			Action:   kShardSetup_In | kShardSetup_SplitIn,
			Updated:  timesec(),
		}
		for _, rep := range lowerShard.shard.Replicas {
			upperShard.Replicas = append(upperShard.Replicas, &kvapi.DatabaseMap_Replica{
				Id:      tm.nextIncr(),
				StoreId: rep.StoreId,
				Action:  kReplicaSetup_In,
			})
		}

		lowerShard.shard.Action |= kShardSetup_SplitOut
		lowerShard.shard.Version = mapData.Version
		lowerShard.shard.Updated = timesec()

		newShards := make([]*kvapi.DatabaseMap_Shard, len(mapData.Shards)+1)
		for j, shard := range mapData.Shards {
			if shard.Id == lowerShard.shard.Id {
				copy(newShards[:j+1], mapData.Shards[:j+1])
				newShards[j+1] = upperShard
				copy(newShards[j+2:], mapData.Shards[j+1:])
				mapData.Shards = newShards
				break
			}
		}

		if mapData.IncrId < tm.mapData.IncrId {
			mapData.IncrId = tm.mapData.IncrId
		}

		it.auditLogger.Put("split", "database %s, shard %d, upper-shard %d, new", tm.data.Name, lowerShard.shard.Id, upperShard.Id)

		return upperShard
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

		if chg := splitCheck(tm, &mapData); chg {
			if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			}
			return
		}

		if splitPending {
			return
		}

		upperShard := splitSchedule(tm, &mapData)
		if upperShard == nil {
			return
		}

		hlog.Printf("info", "job updated, database %s, version %d, shards %d",
			tm.data.Name, upperShard.Version, len(mapData.Shards))

		if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			return
		}

		for _, rep := range upperShard.Replicas {

			if _, ok := tm.replicas[rep.Id]; ok {
				continue
			}

			store := tm.storeMgr.store(rep.StoreId)
			if store == nil {
				continue
			}

			repInst, err := NewDatabase(store, tm.data.Id, tm.data.Name, upperShard.Id, rep.Id, tm.cfg, tm.incrMgr)
			if err != nil {
				continue
			}

			tm.replicas[rep.Id] = repInst
			tm.arrReplicas = append(tm.arrReplicas, repInst)

			tm.setReplicaStatus(rep.Id, func(msRep *kvapi.DatabaseMapStatus_Replica) {
				msRep.Action = 0
				msRep.Updated = timesec()
			})
		}
	}

	it.dbMapMgr.iter(dbAction)

	return nil
}
