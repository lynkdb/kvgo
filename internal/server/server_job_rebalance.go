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

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

const jobRebalanceName = "job-rebalance"

func (it *dbServer) jobReplicaRebalanceSetup(force bool) error {
	return nil

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	// storage check
	var (
		vs = it.storeMgr.stats(jobRebalanceName)
	)

	rebalanceCheck := func(tm *tableMap, mapData *kvapi.TableMap) (chg bool) {

		chgVersion := mapData.Version + 1

		for _, shard := range mapData.Shards {

			if !kvapi.AttrAllow(shard.Action, kShardSetup_Rebalance) {
				continue
			}

			var (
				inReady = 0
				out     = 0
			)

			// move-in
			for _, rep := range shard.Replicas {

				repInst := tm.replica(rep.Id)
				if repInst == nil {
					continue
				}

				if repInst.status.mapVersion != shard.Version ||
					!kvapi.AttrAllow(repInst.status.action, kReplicaStatus_Ready) {
					continue
				}

				if kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveIn) {
					shard.Version = chgVersion
					rep.Action, chg = kReplicaSetup_In, true

					it.auditLogger.Put("rebalance", "shard %d, rep %d, move-in done", shard.Id, rep.Id)
				}

				if kvapi.AttrAllow(rep.Action, kReplicaSetup_In) &&
					!kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
					inReady += 1
				}
			}

			if inReady < int(tm.data.ReplicaNum) {
				continue
			}

			// move-out
			for _, rep := range shard.Replicas {

				if !kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
					continue
				}

				out += 1
				shard.Version = chgVersion
				rep.Action, chg = kReplicaSetup_Out|kReplicaSetup_Remove, true

				it.auditLogger.Put("rebalance", "shard %d, rep %d, move-out done",
					shard.Id, rep.Id)
			}

			if out > 0 {
				sort.Slice(shard.Replicas, func(i, j int) bool {
					return shard.Replicas[i].Action < shard.Replicas[j].Action
				})
			}

			if inReady+out == len(shard.Replicas) {
				shard.Version = chgVersion
				shard.Updated = timesec()
				shard.Action, chg = kvapi.AttrRemove(shard.Action, kShardSetup_Rebalance), true

				it.auditLogger.Put("rebalance", "shard %d, done", shard.Id)
			}
		}

		if chg {
			mapData.Version = chgVersion
		}

		return chg
	}

	type rebalanceEntry struct {
		shard      *kvapi.TableMap_Shard
		dstReplica *kvapi.TableMap_Replica
	}

	rebalanceSchedule := func(tm *tableMap, mapData *kvapi.TableMap) (*rebalanceEntry, bool) {

		if len(vs.Items) < 2 {
			return nil, false
		}

		vs.sort()
		if absFloat64(vs.Items[0].UsedRatio-vs.UsedRatioAvg) < replicaRebalance_StoreCapacityThreshold {
			return nil, false
		}

		// shards
		for _, shard := range mapData.Shards {

			if shard.Action != kShardSetup_In {
				return nil, false
			}

			ss := tm.shardStatus(shard, shard.Version, kReplicaSetup_In, kReplicaStatus_Ready)
			if len(ss.replicas) < int(tm.data.ReplicaNum) {
				return nil, false
			}
		}

		var (
			hitStore   = vs.Items[len(vs.Items)-1]
			hitShard   *kvapi.TableMap_Shard
			srcReplica *kvapi.TableMap_Replica
			tn         = timesec()
		)

		storeContains := func(shard *kvapi.TableMap_Shard, storeId uint64) bool {
			for _, rep := range shard.Replicas {
				if rep.StoreId == storeId {
					return true
				}
			}
			return false
		}

		for _, shard := range mapData.Shards {

			if storeContains(shard, vs.Items[0].status.Id) {
				continue
			}

			if len(shard.Replicas) > int(tm.data.ReplicaNum) {
				continue
			}

			if shard.Updated+kJob_ShardResetIntervalSeconds > tn {
				continue
			}

			ss := tm.shardStatus(shard, shard.Version, kReplicaSetup_In, kReplicaStatus_Ready)
			if len(ss.replicas) < int(tm.data.ReplicaNum) {
				continue
			}

			if ss.avgSize < shardSplit_CapacitySize_Min ||
				ss.avgSize > shardSplit_CapacitySize_Max {
				// continue
			}

			for _, rep := range shard.Replicas {
				if rep.StoreId == hitStore.status.Id {
					hitShard, srcReplica = shard, rep
					break
				}
			}

			if srcReplica != nil {
				break
			}
		}

		if srcReplica == nil {
			return nil, false
		}
		// jsonPrint("rebalance stores", vs)

		srcReplica.Action |= kReplicaSetup_MoveOut

		mapData.Version += 1

		dstReplica := &kvapi.TableMap_Replica{
			Id:      tm.nextIncr(),
			Prev:    srcReplica.Id,
			StoreId: vs.Items[0].status.Id,
			Action:  kReplicaSetup_MoveIn,
		}

		hitShard.Replicas = append(hitShard.Replicas, dstReplica)

		hitShard.Version = mapData.Version
		hitShard.Action |= kShardSetup_Rebalance
		hitShard.Updated = timesec()

		it.auditLogger.Put("rebalance", "shard %d, replica:store %d:%d move-to %d:%d, start",
			hitShard.Id, srcReplica.Id, srcReplica.StoreId, dstReplica.Id, dstReplica.StoreId)

		return &rebalanceEntry{
			shard:      hitShard,
			dstReplica: dstReplica,
		}, true
	}

	tableAction := func(tm *tableMap) {

		it.jobSetupMut.Lock()
		defer it.jobSetupMut.Unlock()

		var (
			prevVersion = tm.mapMeta.Version
			mapData     kvapi.TableMap
			chg         bool
		)

		if err := objectClone(tm.mapData, &mapData); err != nil {
			return
		}

		if chg = rebalanceCheck(tm, &mapData); chg {
			if err := it.mapFlush(tm, &mapData, prevVersion); err != nil {
				return
			}
		}

		reEntry, chg := rebalanceSchedule(tm, &mapData)
		if !chg || reEntry == nil {
			return
		}

		if err := it.mapFlush(tm, &mapData, prevVersion); err != nil {
			return
		}

		store := tm.storeMgr.store(reEntry.dstReplica.StoreId)
		if store == nil {
			return
		}

		trep, err := NewTable(store, tm.data.Id, tm.data.Name, reEntry.shard.Id, reEntry.dstReplica.Id, tm.cfg)
		if err != nil {
			return
		}

		tm.replicas[reEntry.dstReplica.Id] = trep
		tm.arrReplicas = append(tm.arrReplicas, trep)

		// jsonPrint("rebalance stores", vs)
	}

	it.tableMapMgr.iter(tableAction)

	return nil
}
