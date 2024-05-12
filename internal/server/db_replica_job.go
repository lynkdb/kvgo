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
	"fmt"
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func (it *dbReplica) _jobCleanTTL() error {

	var (
		tn           = time.Now().UnixNano() / 1e6
		offset       = keyExpireEncode(0, nil)
		cutset       = keyExpireEncode(tn, nil)
		statsKeys    int64
		statsRawKeys int64
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: offset,
		UpperKey: cutset,
	})
	if err != nil {
		return err
	}
	defer iter.Release()

	batch := it.store.NewBatch()

	for ok := iter.SeekToFirst(); ok && !it.close; ok = iter.Next() {

		if bytes.Compare(iter.Key(), offset) < 0 {
			hlog.Printf("info", "ttl skip %v", iter.Key())
			continue
		}

		if bytes.Compare(iter.Key(), cutset) > 0 {
			hlog.Printf("info", "ttl break %v", iter.Key())
			break
		}

		statsKeys += 1
		statsRawKeys += 1

		logMeta, err := kvapi.LogDecode(bytesClone(iter.Value()))
		if err != nil || len(iter.Key()) < 9 {
			batch.Delete(bytesClone(iter.Key()))
			hlog.Printf("warn", "db err %s", err.Error())
			continue
		}

		if logMeta.ReplicaId == it.replicaId {
			if meta, _ := it.getMeta(logMeta.Key); meta != nil {
				if logMeta.Version >= meta.Version {
					batch.Delete(keyEncode(nsKeyMeta, logMeta.Key))
					batch.Delete(keyEncode(nsKeyData, logMeta.Key))
					statsRawKeys += 2
				}
			}
		}

		batch.Delete(bytesClone(iter.Key()))

		if batch.Len() >= 10000 {

			hlog.Printf("info", "database %s, ttl clean %d, stats %d/%d", it.dbName, batch.Len(), statsKeys, statsRawKeys)

			if ss := batch.Apply(nil); !ss.OK() {
				hlog.Printf("info", "database %s, ttl clean fail %s", it.dbName, ss.Error().Error())
				return ss.Error()
			}
			batch.Clear()
		}
	}

	if batch.Len() > 0 {
		hlog.Printf("debug", "database %s, ttl clean %d, stats %d/%d", it.dbName, batch.Len(), statsKeys, statsRawKeys)
		if ss := batch.Apply(nil); !ss.OK() {
			hlog.Printf("info", "database %s, ttl clean fail %s", it.dbName, ss.Error().Error())
			return ss.Error()
		}
	}

	return nil
}

func (it *dbReplica) _jobCleanLog() error {

	var (
		offset    = keyLogEncode(0, it.replicaId, 0)
		cutset    = keyLogEncode(1<<61, it.replicaId, 0)
		retenTime = timems() - (kLogRetentionSeconds * 1e3)
		retenId   uint64
		statsKeys int64
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: offset,
		UpperKey: cutset,
	})
	if err != nil {
		return err
	}
	defer iter.Release()

	batch := it.store.NewBatch()

	for ok := iter.SeekToFirst(); ok && !it.close; ok = iter.Next() {

		if bytes.Compare(iter.Key(), offset) < 0 {
			hlog.Printf("info", "log-ttl skip %v", iter.Key())
			continue
		}

		if bytes.Compare(iter.Key(), cutset) > 0 {
			hlog.Printf("info", "log-ttl break %v", iter.Key())
			break
		}

		if logMeta, err := kvapi.LogDecode(iter.Value()); err == nil && logMeta.Created >= retenTime {
			break
		} else {
			retenId = logMeta.Id
		}

		statsKeys += 1
		batch.Delete(bytesClone(iter.Key()))

		if batch.Len() >= 10000 {

			hlog.Printf("info", "database %s, log-ttl clean %d, stats %d", it.dbName, batch.Len(), statsKeys)

			if ss := batch.Apply(nil); !ss.OK() {
				hlog.Printf("info", "database %s, log-ttl clean fail %s", it.dbName, ss.Error().Error())
				return ss.Error()
			}
			batch.Clear()
		}
	}

	if batch.Len() > 0 {
		// hlog.Printf("info", "database %s, log-ttl clean %d, stats %d", it.dbName, batch.Len(), statsKeys)
		if ss := batch.Apply(nil); !ss.OK() {
			hlog.Printf("info", "database %s, log-ttl clean fail %s", it.dbName, ss.Error().Error())
			return ss.Error()
		}
	}

	if statsKeys > 0 && retenId > 0 {
		if err := it.store.ExpCompact(offset, keyLogEncode(retenId, it.replicaId, 0)); err != nil {
		}
	}

	it.logMu.Lock()
	defer it.logMu.Unlock()

	_, err = it.logSync(0, retenId)

	return err
}

func (it *dbReplica) _jobLogPull(
	tm *dbMap,
	shard *kvapi.DatabaseMap_Shard,
	lowerKey, upperKey []byte,
	src *dbReplica,
	sts *dbReplicaStatusItem) error {
	//
	if it.dbId != src.dbId ||
		it.shardId != src.shardId ||
		it.replicaId == src.replicaId {
		return nil
	}

	taskKey := fmt.Sprintf("log-pull-%d", src.replicaId)
	if _, ok := it.taskMut.LoadOrStore(taskKey, "true"); ok {
		return nil
	}
	defer it.taskMut.Delete(taskKey)

	var (
		logPullState logPullReplicaState
		nsPullKey    = keySysLogPullOffset(it.replicaId, src.replicaId)
		fetchNum     int64
		flushNum     int64
		err          error
		mapVersion   = shard.Version
	)

	if rs := it.store.Get(nsPullKey, nil); !rs.OK() {
		if !rs.NotFound() {
			return rs.Error()
		}
		logPullState.ShardId = it.shardId
		logPullState.ReplicaId = it.replicaId
		logPullState.SrcReplicaId = src.replicaId

		if rs0 := it.store.Put(nsPullKey, jsonEncode(&logPullState), nil); !rs0.OK() {
			return rs0.Error()
		}

		// jsonPrint("logpull offset", logPullState)
	} else {
		if err = jsonDecode(rs.Bytes(), &logPullState); err != nil {
			return err
		}
		hlog.Printf("debug", "database %s, shard %d, replica %d <- %d, offset %d",
			it.dbId, it.shardId, it.replicaId, src.replicaId, logPullState.SrcLogOffset)

		// jsonPrint("logpull offset", logPullState)
	}

	if !logPullState.FullScan {

		if rs := tm.shardReplicaStatusList(shard); len(rs) >= int(tm.data.ReplicaNum) {
			var (
				sizRel = int64(0)
				numRel = int64(0)
				sizDst = int64(-1)
			)
			for repId, status := range rs {
				if it.replicaId == repId {
					sizDst = status.Used
				} else {
					sizRel += status.Used
					numRel += 1
				}
			}
			if sizDst >= 0 && numRel > 0 {
				sizRel = sizRel / numRel
				diff := float64(sizDst) / float64(sizRel)
				if diff < 0.8 {
					logPullState.FullScan = true
					logPullState.SrcKeyOffset = nil
					hlog.Printf("info", "database %s, shard %d, replica %d <- %d, diff %.2f, reset fullscan",
						tm.data.Name, it.shardId, it.replicaId, src.replicaId, diff)
				}
			}
		}
	}

	// delta
	for !it.close && !logPullState.FullScan {
		//
		rs1, err := src.logRange(&kvapi.LogRangeRequest{
			LowerLog:  logPullState.SrcLogOffset,
			ReplicaId: src.replicaId,
		})
		if err != nil {
			return err
		}

		// jsonPrint("logpull delta offset", logPullState)

		if rs1.LogOffsetOutrange {
			logPullState.FullScan = true
			logPullState.SrcKeyOffset = nil
			logPullState.SrcLogOffset = rs1.LogOffset
			testPrintf("log-pull %d -> %d delta skip, try full sync ...", src.replicaId, it.replicaId)
			break
		}

		fetchNum += int64(len(rs1.Items))

		var (
			logs        = map[string]*kvapi.LogMeta{}
			dataRequest = &kvapi.ReadRequest{}
		)

		for _, item := range rs1.Items {

			nsKey := uint8(0)
			if kvapi.AttrAllow(item.Attrs, kvapi.Write_Attrs_IgnoreMeta) {
				nsKey = nsKeyData
			} else {
				nsKey = nsKeyMeta
			}

			meta, err := it.getRawMeta(nsKey, item.Key)
			if err != nil {
				return err
			}
			if meta == nil {
				if kvapi.AttrAllow(item.Attrs, kvapi.Write_Attrs_Delete) {
					continue
				}
				if item.Expired > 0 && item.Expired <= timems() {
					continue
				}
			} else if item.Version <= meta.Version {
				continue
			}

			logs[string(item.Key)] = item
			dataRequest.Keys = append(dataRequest.Keys, item.Key)
		}

		for len(dataRequest.Keys) > 0 && !it.close {
			rs2 := src.Read(dataRequest)
			if !rs2.OK() && !rs2.NotFound() {
				return rs2.Error()
			}
			for _, item := range rs2.Items {
				logMeta, ok := logs[string(item.Key)]
				if !ok {
					continue
				}
				if item.Meta.Version != logMeta.Version {
					continue
				}

				writeRequest := &kvapi.WriteRequest{
					Key:   item.Key,
					Meta:  item.Meta,
					Value: item.Value,
				}
				if rs0 := it.write(writeRequest, item.Meta.Version); !rs0.OK() {
					return rs0.Error()
				}
				flushNum += 1
			}
			dataRequest.Keys = rs2.NextKeys
		}

		if len(rs1.Items) > 0 {
			logPullState.SrcLogOffset = rs1.Items[len(rs1.Items)-1].Id
			if rs0 := it.store.Put(nsPullKey, jsonEncode(&logPullState), nil); !rs0.OK() {
				return rs0.Error()
			}
			hlog.Printf("debug", "database %s, shard %d, replica %d < %d, offset %d",
				it.dbId, it.shardId, it.replicaId, src.replicaId, logPullState.SrcLogOffset)

			// testPrintf("logpull delta, db %s, shard %d, replica %d to %d, fetch %d flush %d, offset %d",
			// 	it.dbName, it.shardId, src.replicaId, it.replicaId,
			// 	fetchNum, flushNum, logPullState.SrcLogOffset)
		}

		//
		if !rs1.NextResultSet {
			break
		}
	}

	if !logPullState.FullScan {
		sts.mapVersion, sts.value = mapVersion, int64(kReplicaStatus_Ready)
		// jsonPrint("logpull offset", logPullState)
		return nil
	}

	if bytes.Compare(logPullState.SrcKeyOffset, shard.LowerKey) < 0 {
		logPullState.SrcKeyOffset = bytesClone(shard.LowerKey)
	}

	var (
		keyCutset = append(bytesClone(upperKey), bytes.Repeat([]byte{0xff}, 64)...)
	)

	// full scan
	for !it.close {
		//
		rs2, err := src.logKeyRangeMeta(&kvapi.LogKeyRangeRequest{
			LowerKey: logPullState.SrcKeyOffset,
			UpperKey: keyCutset,
		})
		if err != nil {
			return err
		}

		jsonPrint(fmt.Sprintf("log-pull %d -> %d, full offset", src.replicaId, it.replicaId), logPullState)

		fetchNum += int64(len(rs2.Items))

		var (
			dataRequest = &kvapi.ReadRequest{}
		)

		for _, item := range rs2.Items {
			if len(item.Key) < 1 {
				continue
			}

			meta, err := it.getMeta(item.Key)
			if err != nil {
				return err
			}

			if meta == nil || meta.Version < item.Meta.Version {
				dataRequest.Keys = append(dataRequest.Keys, item.Key)
			}

			logPullState.SrcKeyOffset = item.Key
		}

		for len(dataRequest.Keys) > 0 && !it.close {
			rs3 := src.Read(dataRequest)
			if !rs3.OK() && !rs3.NotFound() {
				return rs3.Error()
			}
			for _, item := range rs3.Items {
				writeRequest := &kvapi.WriteRequest{
					Key:   item.Key,
					Meta:  item.Meta,
					Value: item.Value,
				}
				if rs0 := it.write(writeRequest, item.Meta.Version); !rs0.OK() {
					return rs0.Error()
				}
				flushNum += 1
			}
			dataRequest.Keys = rs3.NextKeys
		}

		if len(rs2.Items) > 0 {
			if rs0 := it.store.Put(nsPullKey, jsonEncode(&logPullState), nil); !rs0.OK() {
				return rs0.Error()
			}
			hlog.Printf("info", "database %s, shard %d, replica %d < %d, offset %v",
				it.dbId, it.shardId, it.replicaId, src.replicaId, logPullState.SrcKeyOffset)
			testPrintf("logpull full fetch %d flush %d", fetchNum, flushNum)
		}

		//
		if !rs2.NextResultSet {
			break
		}
	}

	logPullState.FullScan = false

	jsonPrint("logpull offset", logPullState)

	if rs0 := it.store.Put(nsPullKey, jsonEncode(&logPullState), nil); !rs0.OK() {
		return rs0.Error()
	}

	sts.mapVersion, sts.value = mapVersion, int64(kReplicaStatus_Ready)

	return nil
}
