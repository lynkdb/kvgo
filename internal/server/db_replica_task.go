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
	"errors"
	"time"

	"github.com/hooto/hlog4g/hlog"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func (it *dbReplica) _task_deleteRange(ds *dbServer,
	shard *kvapi.DatabaseMap_Shard, lowerKey, upperKey []byte,
) error {
	if it.close || it.store == nil {
		return errors.New("logic deny")
	}

	type nsItem struct {
		name  string
		lower []byte
		upper []byte
	}

	for _, item := range []nsItem{
		{
			"meta",
			keyEncode(nsKeyMeta, lowerKey),
			keyEncode(nsKeyMeta, upperKey),
		},
		{
			"data",
			keyEncode(nsKeyData, lowerKey),
			keyEncode(nsKeyData, upperKey),
		},
	} {
		rs := it.store.DeleteRange(item.lower, item.upper, nil)
		testPrintf("shard %d replica %d, ns %s, remove key-range %v ~ %v, result %v",
			it.shardId, it.replicaId, item.name, item.lower, item.upper, rs.OK())

		if !rs.OK() {
			return rs.Error()
		}

		if err := it.store.ExpCompact(item.lower, item.upper); err == nil {
			testPrintf("shard %d replica %d, ns %s, remove key-range %v ~ %v, compact ok",
				it.shardId, it.replicaId, item.name, item.lower, item.upper)
		}
	}

	ds.auditLogger.Put("range-delete", "database %s:%s, shard %d, rep %d, key-range deleted",
		it.dbName, it.dbId, shard.Id, it.replicaId)

	it.localStatus.storageUsed.mapVersion = 0

	return nil
}

func (it *dbReplica) taskStatusRefresh(
	tm *dbMap,
	shard *kvapi.DatabaseMap_Shard, lowerKey, upperKey []byte, forceRefresh bool,
) error {
	if it.close || it.store == nil {
		return nil
	}

	// testPrintf("shard %d, replica status refresh %d", it.shardId, it.replicaId)

	forceRefresh = false

	var (
		tn = timesec()
	)

	refreshStorageUsed := func() error {

		if (kvapi.AttrAllow(shard.Action, kShardSetup_SplitIn) ||
			kvapi.AttrAllow(shard.Action, kShardSetup_SplitOut) ||
			kvapi.AttrAllow(shard.Action, kShardSetup_Rebalance)) && absInt64(tn-it.localStatus.storageUsed.updated) > 60 {
			//
		} else if !forceRefresh &&
			it.localStatus.storageUsed.mapVersion == shard.Version &&
			it.localStatus.storageUsed.updated+dbReplicaStatusRefresh_IntervalSecond > tn &&
			it.localStatus.kvWriteSize.Load() < kShardSplit_CapacitySize_Fresh {
			// testPrintf("replica %d, version diff %v, sec %d, kv-write-size %d %d",
			// 	it.replicaId,
			// 	it.localStatus.storageUsed.mapVersion == shard.Version,
			// 	tn-it.localStatus.storageUsed.updated,
			// 	it.localStatus.kvWriteKeys.Load(), it.localStatus.kvWriteSize.Load()/(1<<20))
			return nil
		}

		// testPrintf("replica %d, kv-write-size %d %d",
		// 	it.replicaId, it.localStatus.kvWriteKeys.Load(), it.localStatus.kvWriteSize.Load()/(1<<20))

		var (
			lowerKey = bytesClone(lowerKey)
			upperKey = append(bytesClone(upperKey), 0xff)
		)

		rs, err := it.store.SizeOf([]*storage.IterOptions{
			{
				LowerKey: keyEncode(nsKeyData, lowerKey),
				UpperKey: keyEncode(nsKeyData, upperKey),
			},
		})
		if err != nil {
			testPrintf("size of %v", err)
			return err
		}

		// testPrintf("replica status refresh : rep %d:%d ver %d+, write-size %d db-size %d, key %v",
		// 	shard.Id, it.replicaId, int64(shard.Version)-int64(it.localStatus.storageUsed.mapVersion),
		// 	it.localStatus.kvWriteSize.Load()/(1<<20), rs[0]/(1<<20),
		// 	string(lowerKey))

		ir := float64(0)
		if r := it.localStatus.storageUsed.value; r > 0 {
			ir = float64(absInt64((rs[0]/(1<<20))-r)) / float64(r)
		}

		it.localStatus.storageUsed.mapVersion = shard.Version
		it.localStatus.storageUsed.updated = tn

		if ir > 0.05 ||
			it.localStatus.storageUsed.countUpdated+dbReplicaStatusRefresh_KeyStatIntervalSecond < tn {

			it.localStatus.storageUsed.value = rs[0] / (1 << 20)
			it.localStatus.storageUsed.countUpdated = tn
			t0 := time.Now()

			if rs, err := it.store.KeyStats(&storage.IterOptions{
				LowerKey: keyEncode(nsKeyMeta, lowerKey),
				UpperKey: keyEncode(nsKeyMeta, upperKey),
			}); err == nil {
				it.localStatus.storageUsed.count = rs.Keys
			}

			it.localStatus.storageUsed.keyStats = []*kvapi.DatabaseMapStatus_KeyStat{}
			for _, ks := range tm.data.KeyStats {
				if len(ks.Key) == 0 {
					continue
				}
				if bytes.Compare(ks.Key, lowerKey) < 0 ||
					bytes.Compare(ks.Key, upperKey) > 0 {
					continue
				}

				smb, err := it.store.SizeOf([]*storage.IterOptions{
					{
						LowerKey: keyEncode(nsKeyData, ks.Key),
						UpperKey: keyEncode(nsKeyData, append(ks.Key, 0xff)),
					},
				})
				if err != nil {
					continue
				}

				if rs, err := it.store.KeyStats(&storage.IterOptions{
					LowerKey: keyEncode(nsKeyMeta, ks.Key),
					UpperKey: keyEncode(nsKeyMeta, append(ks.Key, 0xff)),
				}); err == nil {
					it.localStatus.storageUsed.keyStats = append(it.localStatus.storageUsed.keyStats, &kvapi.DatabaseMapStatus_KeyStat{
						Key:    ks.Key,
						Num:    rs.Keys,
						SizeMb: smb[0] / (1 << 20),
					})
				}
			}
			hlog.Printf("info", "db %s, replica key-stats refresh rep %d:%d, chg-rate %.2f, time in %v",
				tm.data.Name, shard.Id, it.replicaId, ir, time.Since(t0))
		}

		it.localStatus.kvWriteKeys.Store(0)
		it.localStatus.kvWriteSize.Store(0)

		return nil
	}

	if err := refreshStorageUsed(); err != nil {
		return err
	}

	return nil
}
