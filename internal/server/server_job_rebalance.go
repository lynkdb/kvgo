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

const jobRebalanceName = "job-rebalance"

func (it *dbServer) jobReplicaRebalanceSetup(force bool) error {

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

	it.storeMgr.rebalancePending = 0

	rebalanceCheck := func(tm *dbMap, mapData *kvapi.DatabaseMap) bool {

		var (
			chg            = false
			nextMapVersion = mapData.Version + 1
		)

		for _, shard := range mapData.Shards {

			if !kvapi.AttrAllow(shard.Action, kShardSetup_Rebalance) {
				continue
			}

			it.storeMgr.rebalancePending += 1

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

				if repInst.localStatus.action.mapVersion != shard.Version ||
					!kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Ready) {
					continue
				}

				if kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveIn) {
					rep.Action, chg = kReplicaSetup_In, true
					it.auditLogger.Put("rebalance", "shard %d, rep %d, move-in done", shard.Id, rep.Id)
				}

				if kvapi.AttrAllow(rep.Action, kReplicaSetup_In) &&
					!kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
					inReady += 1
				}
			}

			if inReady >= int(tm.data.ReplicaNum) {

				// move-out
				for _, rep := range shard.Replicas {

					if !kvapi.AttrAllow(rep.Action, kReplicaSetup_MoveOut) {
						continue
					}

					out += 1

					rep.Action, chg = kReplicaSetup_Out|kReplicaSetup_Remove, true

					it.auditLogger.Put("rebalance", "shard %d, rep %d, move-out done",
						shard.Id, rep.Id)
				}

				if out > 0 {
					sort.Slice(shard.Replicas, func(i, j int) bool {
						return shard.Replicas[i].Action < shard.Replicas[j].Action
					})
				}
			}

			if inReady+out == len(shard.Replicas) {

				shard.Action, chg = kvapi.AttrRemove(shard.Action, kShardSetup_Rebalance), true

				it.storeMgr.rebalancePending -= 1
				it.auditLogger.Put("rebalance", "shard %d, done", shard.Id)
			}

			if chg {
				mapData.Version, shard.Version = nextMapVersion, nextMapVersion
				shard.Updated = timesec()
				break
			}
		}

		return chg
	}

	type rebalanceEntry struct {
		shard      *kvapi.DatabaseMap_Shard
		dstReplica *kvapi.DatabaseMap_Replica
	}

	storeContains := func(shard *kvapi.DatabaseMap_Shard, storeId uint64) bool {
		for _, rep := range shard.Replicas {
			if rep.StoreId == storeId {
				return true
			}
		}
		return false
	}

	rebalanceSchedule := func(tm *dbMap, mapData *kvapi.DatabaseMap) *rebalanceEntry {

		// shards
		for _, shard := range mapData.Shards {

			if shard.Action != kShardSetup_In {
				return nil
			}

			ss := tm.shardStatus(shard, shard.Version, kReplicaSetup_In, kReplicaStatus_Ready)
			if len(ss.replicas) < int(tm.data.ReplicaNum) {
				return nil
			}
		}

		if vs == nil || len(vs.Items) < 2 {
			return nil
		}

		sort.Slice(vs.Items, func(i, j int) bool {
			return vs.Items[i].Free < vs.Items[j].Free
		})

		var (
			moveSize  = tm.shardSize() * 4
			hotStore  = vs.Items[0]
			coldStore = vs.Items[len(vs.Items)-1]
			diffAbs   = absInt64(hotStore.Free - coldStore.Free)
			diffRatio = absFloat64(hotStore.FreeRatio - coldStore.FreeRatio)
		)

		if diffAbs < moveSize {
			// diffRatio < kReplicaRebalance_StoreCapacityThreshold {
			hlog.Printf("debug", "db %s, skip free-ratio %.4f, free-size %d MiB",
				tm.data.Name, diffRatio, diffAbs)
			return nil
		}

		hlog.Printf("debug", "db %s, hit free-ratio %.4f, free-size %d MiB, store %d -> %d",
			tm.data.Name, diffRatio, diffAbs, hotStore.status.Id, coldStore.status.Id)

		tryMove := func(
			tm *dbMap, mapData *kvapi.DatabaseMap,
			hotStore, coldStore *storeStatsItem,
		) *rebalanceEntry {

			var (
				hitShard   *kvapi.DatabaseMap_Shard
				srcReplica *kvapi.DatabaseMap_Replica
			)

			for _, shard := range mapData.Shards {

				if shard.Action != kShardSetup_In {
					continue
				}

				if !storeContains(shard, hotStore.status.Id) ||
					storeContains(shard, coldStore.status.Id) {
					continue
				}

				if len(shard.Replicas) > int(tm.data.ReplicaNum) {
					continue
				}

				if shard.Updated+kReplicaRebalance_JobIntervalSeconds > tn {
					// hlog.Printf("info", "db %s, skip time %d", tm.data.Name, tn-shard.Updated)
					continue
				}

				ss := tm.shardStatus(shard, shard.Version, kReplicaSetup_In, kReplicaStatus_Ready)
				if len(ss.replicas) < int(tm.data.ReplicaNum) {
					continue
				}

				if ss.avgSize < kShardSplit_CapacitySize_Min ||
					ss.avgSize > kShardSplit_CapacitySize_Max {
					hlog.Printf("info", "db %s, skip size %d", tm.data.Name, ss.avgSize)
					continue
				}

				for _, rep := range shard.Replicas {
					if rep.StoreId == hotStore.status.Id {
						hitShard, srcReplica = shard, rep
						break
					}
				}

				if srcReplica != nil {
					break
				}
			}

			if srcReplica == nil {
				hlog.Printf("debug", "db %s, hot store miss", tm.data.Name)
				return nil
			}

			hlog.Printf("info", "db %s, hot store %v", tm.data.Name, vs)

			srcReplica.Action |= kReplicaSetup_MoveOut

			mapData.Version += 1

			dstReplica := &kvapi.DatabaseMap_Replica{
				Id:      tm.nextIncr(),
				Prev:    srcReplica.Id,
				StoreId: coldStore.status.Id,
				Action:  kReplicaSetup_MoveIn,
			}

			hitShard.Replicas = append(hitShard.Replicas, dstReplica)

			hitShard.Version = mapData.Version
			hitShard.Action |= kShardSetup_Rebalance
			hitShard.Updated = timesec()

			it.storeMgr.lastRebalanceUpdated = tn
			it.storeMgr.rebalancePending += 1

			it.auditLogger.Put("rebalance", "shard %d, replica:store %d:%d move-to %d:%d, start",
				hitShard.Id, srcReplica.Id, srcReplica.StoreId, dstReplica.Id, dstReplica.StoreId)

			return &rebalanceEntry{
				shard:      hitShard,
				dstReplica: dstReplica,
			}
		}

		if re := tryMove(tm, mapData, hotStore, coldStore); re != nil {
			return re
		}

		if len(vs.Items) <= 2 {
			return nil
		}

		for i := 0; i < len(vs.Items)-1; i++ {

			hotStore = vs.Items[i]

			for j := len(vs.Items) - 1; j > i; j-- {

				coldStore = vs.Items[j]

				diffAbs = absInt64(hotStore.Free - coldStore.Free)
				diffRatio = absFloat64(hotStore.FreeRatio - coldStore.FreeRatio)

				if diffAbs < moveSize { //&&
					// diffRatio < kReplicaRebalance_StoreCapacityThreshold {
					hlog.Printf("debug", "db %s, skip free-ratio %.4f, free-size %d MiB, store %d -> %d",
						tm.data.Name, diffRatio, diffAbs, hotStore.status.Id, coldStore.status.Id)
					break
				}

				hlog.Printf("debug", "db %s, hit free-ratio %.4f, free-size %d MiB, store %d -> %d",
					tm.data.Name, diffRatio, diffAbs, hotStore.status.Id, coldStore.status.Id)

				if re := tryMove(tm, mapData, hotStore, coldStore); re != nil {

					hlog.Printf("info", "db %s, hit free-ratio %.4f, free-size %d MiB, store %d -> %d HIT",
						tm.data.Name, diffRatio, diffAbs, hotStore.status.Id, coldStore.status.Id)

					return re
				}
			}
		}

		return nil
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

		if chg := rebalanceCheck(tm, &mapData); chg {
			if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			}
			return
		}

		if it.storeMgr.rebalancePending > 0 {
			return
		}

		if it.storeMgr.lastRebalanceUpdated+kReplicaRebalance_JobIntervalSeconds > tn {
			return
		}

		if vs == nil {
			vs = it.storeMgr.stats(jobRebalanceName)
		}

		reEntry := rebalanceSchedule(tm, &mapData)
		if reEntry == nil {
			return
		}

		if err := it.mapFlush(tm, &mapData, prevMetaVersion); err != nil {
			return
		}

		store := tm.storeMgr.store(reEntry.dstReplica.StoreId, tm.data.Id)
		if store == nil {
			return
		}

		trep, err := NewDatabase(store, tm.data.Id, tm.data.Name, reEntry.shard.Id, reEntry.dstReplica.Id, tm.cfg, tm.incrMgr)
		if err != nil {
			return
		}

		tm.replicas[reEntry.dstReplica.Id] = trep
		tm.arrReplicas = append(tm.arrReplicas, trep)
	}

	it.dbMapMgr.iter(dbAction)

	return nil
}
