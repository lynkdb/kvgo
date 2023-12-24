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

func (it *dbServer) jobShardSplitSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	type replicaItem struct {
		rep  *dbReplica
		size int64
		dist int64
	}

	type shardItem struct {
		shard    *kvapi.DatabaseMap_Shard
		next     *kvapi.DatabaseMap_Shard
		sizeAvg  int64
		replicas []*replicaItem
	}

	splitInCheck := func(tm *dbMap, shard *kvapi.DatabaseMap_Shard) bool {

		if !kvapi.AttrAllow(shard.Action, kShardSetup_SplitIn) {
			return false
		}

		if len(shard.Replicas) < 1 || len(shard.Replicas) < int(tm.data.ReplicaNum) {
			return false
		}

		for _, rep := range shard.Replicas {

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				return false
			}

			if !kvapi.AttrAllow(repInst.status.action, kReplicaStatus_Ready) {
				return false
			}
		}

		shard.Action = kvapi.AttrRemove(shard.Action, kShardSetup_SplitIn)
		shard.Updated = timesec()

		it.auditLogger.Put("split", "database %s, shard %d, split-in done", tm.data.Name, shard.Id)

		return true
	}

	splitOutCheck := func(tm *dbMap, shard *kvapi.DatabaseMap_Shard) bool {

		if !kvapi.AttrAllow(shard.Action, kShardSetup_SplitOut) {
			return false
		}

		if len(shard.Replicas) < 1 || len(shard.Replicas) < int(tm.data.ReplicaNum) {
			return false
		}

		for _, rep := range shard.Replicas {

			repInst := tm.replica(rep.Id)
			if repInst == nil {
				return false
			}

			if !kvapi.AttrAllow(repInst.status.action, kReplicaStatus_Ready) {
				return false
			}

			if repInst.status.mapVersion < shard.Version {
				return false
			}
		}

		shard.Action = kvapi.AttrRemove(shard.Action, kShardSetup_SplitOut)
		shard.Updated = timesec()

		it.auditLogger.Put("split", "database %s, shard %d, split-out done", tm.data.Name, shard.Id)

		return true
	}

	splitAction := func(tm *dbMap, shard *shardItem) *kvapi.DatabaseMap_Shard {

		for _, v := range shard.replicas {
			v.dist = absInt64(shard.sizeAvg - v.size)
		}

		sort.Slice(shard.replicas, func(i, j int) bool {
			return shard.replicas[i].dist < shard.replicas[j].dist
		})

		mk, err := shard.replicas[0].rep.trySplit(shard.shard, shard.next)
		if err != nil {
			testPrintf("spit err %v", err)
			return nil
		}

		it.jobSetupMut.Lock()
		defer it.jobSetupMut.Unlock()

		var mapData kvapi.DatabaseMap
		if err := objectClone(tm.mapData, &mapData); err != nil {
			testPrintf("spit err %v", err)
			return nil
		}

		var lowerShard *kvapi.DatabaseMap_Shard
		for _, v := range mapData.Shards {
			if v.Id == shard.shard.Id {
				lowerShard = v
				break
			}
		}
		if lowerShard == nil {
			testPrintf("spit err nil")
			return nil
		}

		if bytes.Compare(mk, lowerShard.LowerKey) <= 0 {
			return nil
		}

		mapData.Version += 1

		upperShard := &kvapi.DatabaseMap_Shard{
			Id:       tm.nextIncr(),
			Prev:     lowerShard.Id,
			Version:  mapData.Version,
			LowerKey: mk,
			Action:   kShardSetup_In | kShardSetup_SplitIn,
			Updated:  timesec(),
		}
		for _, rep := range lowerShard.Replicas {
			upperShard.Replicas = append(upperShard.Replicas, &kvapi.DatabaseMap_Replica{
				Id:      tm.nextIncr(),
				StoreId: rep.StoreId,
				Action:  kReplicaSetup_In,
			})
		}

		// lowerShard.UpperKey = mk
		lowerShard.Action |= kShardSetup_SplitOut
		lowerShard.Version = mapData.Version
		lowerShard.Updated = timesec()

		mapData.Shards = append(mapData.Shards, upperShard)

		sort.Slice(mapData.Shards, func(i, j int) bool {
			return bytes.Compare(mapData.Shards[i].LowerKey, mapData.Shards[j].LowerKey) < 0
		})

		if mapData.IncrId < tm.mapData.IncrId {
			mapData.IncrId = tm.mapData.IncrId
		}

		wr := kvapi.NewWriteRequest(nsSysDatabaseMap(tm.data.Id), jsonEncode(mapData))
		wr.Database = sysDatabaseName
		wr.PrevVersion = tm.mapMeta.Version

		rs, err := it.api.Write(nil, wr)
		if err != nil {
			hlog.Printf("error", "job update fail %s", err.Error())
			testPrintf("shard split update fail %s", err.Error())
			return nil
		}
		if rs.OK() {

			item := rs.Item()
			if item == nil {
				return nil
			}

			tm.mapMeta = item.Meta
			tm.mapData = &mapData

			for _, rep := range upperShard.Replicas {

				if _, ok := tm.replicas[rep.Id]; ok {
					continue
				}

				store := tm.storeMgr.store(rep.StoreId)
				if store == nil {
					continue
				}

				trep, err := NewDatabase(store, tm.data.Id, tm.data.Name, upperShard.Id, rep.Id, tm.cfg)
				if err != nil {
					continue
				}

				tm.replicas[rep.Id] = trep
				tm.arrReplicas = append(tm.arrReplicas, trep)

				tm.syncStatusReplica(rep.Id, kReplicaStatus_In)
			}

			hlog.Printf("error", "job updated, database %s, version %d, shards %d",
				tm.data.Name, shard.shard.Version, len(mapData.Shards))

			it.auditLogger.Put("split", "database %s, shard %d to %d, split start",
				tm.data.Name, lowerShard.Id, upperShard.Id)

		} else {
			testPrintf("shard split update fail %s", rs.StatusMessage)
		}

		return upperShard
	}

	dbAction := func(tm *dbMap) {

		var (
			shardItems = []*shardItem{}
			tn         = timesec()
		)

		for i, shard := range tm.mapData.Shards {

			if len(shard.Replicas) != int(tm.data.ReplicaNum) {
				// testPrintf("shard %d %d %d", shard.Id, len(shard.Replicas), tm.data.ReplicaNum)
				continue
			}

			if kvapi.AttrAllow(shard.Action, kShardSetup_SplitIn) {
				if chg := splitInCheck(tm, shard); chg {
					testPrintf("shard split-in check %v", chg)
				}
				continue
			}

			if kvapi.AttrAllow(shard.Action, kShardSetup_SplitOut) {
				splitOutCheck(tm, shard)
				continue
			}

			if shard.Action != kShardSetup_In {
				continue
			}

			if shard.Updated+kJob_ShardResetIntervalSeconds > tn {
				// continue
			}

			si := &shardItem{
				shard: shard,
			}

			if i+1 < len(tm.mapData.Shards) {
				si.next = tm.mapData.Shards[i+1]
			}

			for _, rep := range shard.Replicas {

				trep, ok := tm.replicas[rep.Id]
				if !ok {
					continue
				}

				if trep.status.mapVersion != shard.Version {
					si.replicas = nil
					break
				}

				si.sizeAvg += trep.status.storageUsed.value
				si.replicas = append(si.replicas, &replicaItem{
					rep:  trep,
					size: trep.status.storageUsed.value,
				})
			}

			if len(si.replicas) != len(shard.Replicas) {
				continue
			}

			si.sizeAvg = si.sizeAvg / int64(len(si.replicas))
			if si.sizeAvg <= shardSplit_CapacitySize_Def {
				continue
			}

			shardItems = append(shardItems, si)
		}

		if len(shardItems) == 0 {
			return
		}

		sort.Slice(shardItems, func(i, j int) bool {
			return shardItems[i].sizeAvg > shardItems[j].sizeAvg
		})

		splitAction(tm, shardItems[0])
	}

	it.dbMapMgr.iter(dbAction)

	return nil
}
