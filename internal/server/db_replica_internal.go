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
	"errors"
	"fmt"
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func (it *dbReplica) getMeta(key []byte) (*kvapi.Meta, error) {

	ss := it.store.Get(keyEncode(nsKeyMeta, key), nil)

	if !ss.OK() {
		if ss.NotFound() {
			return nil, nil
		}
		return nil, ss.Error()
	}

	meta, _, err := kvapi.MetaDecode(ss.Bytes())
	return meta, err
}

func (it *dbReplica) getRawMeta(nsKey byte, key []byte) (*kvapi.Meta, error) {

	ss := it.store.Get(keyEncode(nsKey, key), nil)

	if !ss.OK() {
		if ss.NotFound() {
			return nil, nil
		}
		return nil, ss.Error()
	}

	meta, _, err := kvapi.MetaDecode(ss.Bytes())
	return meta, err
}

func (it *dbReplica) getData(key []byte) (*kvapi.KeyValue, error) {

	ss := it.store.Get(keyEncode(nsKeyData, key), nil)

	if !ss.OK() {
		if ss.NotFound() {
			return nil, nil
		}
		return nil, ss.Error()
	}

	kv, err := kvapi.KeyValueDecode(ss.Bytes())
	if err == nil {
		kv.Key = key
	}
	return kv, err
}

func (it *dbReplica) logRange(req *kvapi.LogRangeRequest) (*kvapi.LogRangeResponse, error) {

	if req.UpperLog <= req.LowerLog {
		req.UpperLog = 1 << 63
	}

	if req.LowerLog < it.logState.RetentionOffset {
		testPrintf("log-range out retention, replica %d, req log %d, local log %d",
			req.ReplicaId, req.LowerLog, it.logState.RetentionOffset)
		return &kvapi.LogRangeResponse{
			LogOffset:         it.logState.Offset,
			LogOffsetOutrange: true,
		}, nil
	}

	var (
		lowerKey  = keyLogEncode(req.LowerLog, req.ReplicaId, 0)
		upperKey  = keyLogEncode(req.UpperLog, req.ReplicaId, 0)
		limitNum  = 10000
		limitSize = 2 << 20
		readSize  int
		rs        = &kvapi.LogRangeResponse{}
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: lowerKey,
		UpperKey: upperKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Release()

	for ok := iter.SeekToFirst(); ok; ok = iter.Next() {

		if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
			rs.NextResultSet = true
			break
		}

		item, err := kvapi.LogDecode(bytesClone(iter.Value()))
		if err != nil {
			continue
		}
		if item.ReplicaId != req.ReplicaId {
			continue
		}

		readSize += len(iter.Value())
		if len(item.Key) <= 512 {
			rs.Items = append(rs.Items, item)
		}

		if len(rs.Items) >= limitNum || readSize >= limitSize {
			rs.NextResultSet = true
			break
		}
	}

	return rs, nil
}

func (it *dbReplica) logKeyRangeMeta(req *kvapi.LogKeyRangeRequest) (*kvapi.ResultSet, error) {

	var (
		lowerKey  = keyEncode(nsKeyMeta, req.LowerKey)
		upperKey  = keyEncode(nsKeyMeta, req.UpperKey)
		limitNum  = 10000
		limitSize = 2 << 20
		readSize  int
		rs        = &kvapi.ResultSet{}
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: lowerKey,
		UpperKey: upperKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Release()

	for ok := iter.SeekToFirst(); ok && !it.close; ok = iter.Next() {

		if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
			rs.NextResultSet = true
			break
		}

		meta, _, err := kvapi.MetaDecode(bytesClone(iter.Value()))
		if err != nil || meta == nil {
			continue
		}

		readSize += len(iter.Key())
		readSize += len(iter.Value())

		item := &kvapi.KeyValue{
			Key:  bytesClone(iter.Key())[1:],
			Meta: meta,
		}

		if len(item.Key) <= 512 {
			rs.Items = append(rs.Items, item)
		}

		if len(rs.Items) >= limitNum || readSize >= limitSize {
			rs.NextResultSet = true
			break
		}
	}

	return rs, nil
}

func (it *dbReplica) _writePrepare(req *kvapi.WriteProposalRequest) (*kvapi.Meta, error) {

	if it.cfg.Server.MetricsEnable {
		t0 := time.Now()
		defer func() {
			metricCounter.Add(metricService, "Key.Prepare", 1)
			metricLatency.Add(metricService, "Key.Prepare", time.Since(t0).Seconds())
		}()
	}

	if req.Write == nil {
		return nil, errors.New("invalid request")
	}

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	tn := time.Now().UnixNano() / 1e6

	if len(it.proposals) > 10 {
		dels := []string{}
		for k, v := range it.proposals {
			if (v.expired) < tn {
				dels = append(dels, k)
			}
		}
		for _, k := range dels {
			delete(it.proposals, k)
		}
	}

	p, ok := it.proposals[string(req.Write.Key)]
	if ok && p.expired > tn && p.id != req.Id {
		return nil, errors.New("write deny")
	}

	pLog, err := it.versionSync(1, 0)
	if err != nil {
		return nil, err
	}

	pIncNs := req.Write.Meta.IncrNs
	pIncId := req.Write.Meta.IncrId
	if req.Write.IncrNamespace != "" && req.Write.Meta.IncrId == 0 {
		pIncNs, pIncId, err = it.incrMgr.sync(req.Write.IncrNamespace, 1, 0, req.Write.Key)
		if err != nil {
			return nil, err
		}
	}

	it.proposals[string(req.Write.Key)] = &proposalx{
		id:      req.Id,
		expired: tn + writeProposalTTL,
		write:   req,
	}

	return &kvapi.Meta{
		Version: pLog,
		IncrNs:  pIncNs,
		IncrId:  pIncId,
	}, nil
}

func (it *dbReplica) _writeAccept(req *kvapi.WriteProposalRequest) (*kvapi.Meta, error) {

	if it.cfg.Server.MetricsEnable {
		t0 := time.Now()
		defer func() {
			metricCounter.Add(metricService, "Key.Accept", 1)
			metricLatency.Add(metricService, "Key.Accept", time.Since(t0).Seconds())
		}()
	}

	if req.Write.Meta == nil {
		return nil, errors.New("invalid request")
	}

	var (
		tn     = time.Now().UnixNano() / 1e6
		cVerOn = true
		cVer   = req.Write.Meta.Version
		cIncNs = req.Write.Meta.IncrNs
		cIncId = req.Write.Meta.IncrId
	)

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	pp, ok := it.proposals[string(req.Write.Key)]
	if !ok || (pp.expired) < tn || pp.write == nil {
		return nil, errors.New("deny")
	}

	if pp.write.Write.Meta.Version > cVer {
		return nil, errors.New("invalid version")
	}

	it.versionSync(0, cVer)
	if pp.write.Write.IncrNamespace != "" && cIncId > 0 {
		it.incrMgr.sync(pp.write.Write.IncrNamespace, 0, cIncId, pp.write.Write.Key)
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	meta, err := it.getMeta(req.Write.Key)
	if meta == nil && err != nil {
		return nil, err
	}
	if meta != nil && meta.Version > cVer {
		return nil, errors.New("invalid version")
	}

	if req.Write.Meta.IncrId > 0 && kvapi.AttrAllow(req.Write.Attrs, kvapi.Write_Attrs_InnerSync) {
		it.incrMgr.sync("", 0, req.Write.Meta.IncrId, req.Write.Key)
	}

	if pp.write.Write.Meta.Updated < 1 {
		pp.write.Write.Meta.Updated = tn
	}

	// if pp.write.Write.Meta.Created < 1 {
	// 	pp.write.Write.Meta.Created = tn
	// }

	pp.write.Write.Meta.Version = cVer
	pp.write.Write.Meta.IncrNs = cIncNs
	pp.write.Write.Meta.IncrId = cIncId

	bsMeta, bsData, err := pp.write.Write.Encode()
	if err != nil {
		return nil, err
	}
	writeSize := len(bsMeta) + len(bsData)

	batch := it.store.NewBatch()

	if !kvapi.AttrAllow(pp.write.Write.Attrs, kvapi.Write_Attrs_IgnoreMeta) {
		batch.Put(keyEncode(nsKeyMeta, pp.write.Write.Key), bsMeta)
	}
	if !kvapi.AttrAllow(pp.write.Write.Attrs, kvapi.Write_Attrs_IgnoreData) {
		batch.Put(keyEncode(nsKeyData, pp.write.Write.Key), bsData)
	}

	if (cVerOn && !it.cfg.Feature.WriteLogDisable) ||
		pp.write.Write.Meta.Expired > 0 {

		it.logMu.Lock()
		defer it.logMu.Unlock()

		logId, err := it.logSync(1, 0)
		if err != nil {
			return nil, err
		}

		bsLogMeta, err := pp.write.Write.LogEncode(logId, it.replicaId)
		if err != nil {
			return nil, err
		}

		if cVerOn && !it.cfg.Feature.WriteLogDisable &&
			!kvapi.AttrAllow(pp.write.Write.Attrs, kvapi.Write_Attrs_InnerSync) {
			batch.Put(keyLogEncode(logId, it.replicaId, 0), bsLogMeta)
			writeSize += len(bsLogMeta)
		}

		if pp.write.Write.Meta.Expired > 0 {
			batch.Put(keyExpireEncode(pp.write.Write.Meta.Expired, pp.write.Write.Key), bsLogMeta)
			writeSize += len(bsLogMeta)
		}
	}

	wopts := &storage.WriteOptions{}

	if kvapi.AttrAllow(pp.write.Write.Attrs, kvapi.Write_Attrs_Sync) {
		wopts.Sync = true
	}

	if ss := batch.Apply(wopts); !ss.OK() {
		err = ss.Error()
	}

	if err != nil {
		return nil, err
	}

	if it.cfg.Server.MetricsEnable {
		metricCounter.Add(metricStorage, "Key.Write", 1)
		metricCounter.Add(metricStorageSize, "Key.Write", float64(writeSize))
	}

	delete(it.proposals, string(pp.write.Write.Key))

	it.localStatus.kvWriteKeys.Add(1)
	it.localStatus.kvWriteSize.Add(int64(writeSize))

	return &kvapi.Meta{
		Version: cVer,
		IncrNs:  cIncNs,
		IncrId:  cIncId,
	}, nil
}

func (it *dbReplica) _deletePrepare(req *kvapi.DeleteProposalRequest) (*kvapi.Meta, error) {

	if it.cfg.Server.MetricsEnable {
		t0 := time.Now()
		defer func() {
			metricCounter.Add(metricService, "Key.Prepare", 1)
			metricLatency.Add(metricService, "Key.Prepare", time.Since(t0).Seconds())
		}()
	}

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	tn := time.Now().UnixNano() / 1e6

	if len(it.proposals) > 10 {
		dels := []string{}
		for k, v := range it.proposals {
			if (v.expired) < tn {
				dels = append(dels, k)
			}
		}
		for _, k := range dels {
			delete(it.proposals, k)
		}
	}

	p, ok := it.proposals[string(req.Key)]
	if ok && p.expired > tn && p.id != req.Id {
		return nil, errors.New("write deny")
	}

	pLog, err := it.versionSync(1, 0)
	if err != nil {
		return nil, err
	}

	it.proposals[string(req.Key)] = &proposalx{
		id:      req.Id,
		expired: tn + writeProposalTTL,
		delete:  req,
	}

	return &kvapi.Meta{
		Version: pLog,
	}, nil
}

func (it *dbReplica) _deleteAccept(req *kvapi.DeleteProposalRequest) (*kvapi.Meta, error) {

	if it.cfg.Server.MetricsEnable {
		t0 := time.Now()
		defer func() {
			metricCounter.Add(metricService, "Key.Accept", 1)
			metricLatency.Add(metricService, "Key.Accept", time.Since(t0).Seconds())
		}()
	}

	if req.Meta == nil {
		return nil, errors.New("invalid request")
	}

	var (
		tn     = time.Now().UnixNano() / 1e6
		cVerOn = true
		cVer   = req.Meta.Version
	)

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	pp, ok := it.proposals[string(req.Key)]
	if !ok || (pp.expired) < tn || pp.delete == nil {
		return nil, errors.New("deny")
	}

	if pp.delete.Meta.Version > cVer {
		return nil, errors.New("invalid version")
	}

	it.versionSync(0, cVer)

	it.mu.Lock()
	defer it.mu.Unlock()

	meta, err := it.getMeta(req.Key)
	if meta == nil && err != nil {
		return nil, err
	}
	if meta != nil && meta.Version > cVer {
		return nil, errors.New("invalid version")
	}

	if pp.delete.Meta.Updated < 1 {
		pp.delete.Meta.Updated = tn
	}

	// if pp.delete.Meta.Created < 1 {
	// 	pp.delete.Meta.Created = tn
	// }

	pp.delete.Meta.Version = cVer

	batch := it.store.NewBatch()

	if !kvapi.AttrAllow(pp.delete.Attrs, kvapi.Write_Attrs_RetainMeta) {
		batch.Delete(keyEncode(nsKeyMeta, pp.delete.Key))
	}

	batch.Delete(keyEncode(nsKeyData, pp.delete.Key))

	if it.cfg.Server.MetricsEnable {
		metricCounter.Add(metricStorage, "Key.Delete", 1)
		metricCounter.Add(metricStorageSize, "Key.Delete", float64(len(pp.delete.Key)))
	}

	if cVerOn && !it.cfg.Feature.WriteLogDisable &&
		!kvapi.AttrAllow(pp.delete.Attrs, kvapi.Write_Attrs_InnerSync) {

		it.logMu.Lock()
		defer it.logMu.Unlock()

		logId, err := it.logSync(1, 0)
		if err != nil {
			return nil, err
		}

		bsLogMeta, err := pp.delete.LogEncode(logId, cVer, it.replicaId)
		if err != nil {
			return nil, err
		}

		batch.Put(keyLogEncode(logId, it.replicaId, 0), bsLogMeta)
		if it.cfg.Server.MetricsEnable {
			metricCounter.Add(metricStorageSize, "Key.Delete", float64(len(bsLogMeta)))
		}
	}

	wopts := &storage.WriteOptions{}

	if kvapi.AttrAllow(pp.delete.Attrs, kvapi.Write_Attrs_Sync) {
		wopts.Sync = true
	}

	if ss := batch.Apply(wopts); !ss.OK() {
		err = ss.Error()
	}

	if err != nil {
		return nil, err
	}

	delete(it.proposals, string(pp.delete.Key))

	it.localStatus.kvWriteKeys.Add(1)
	if meta != nil && meta.Size > 0 {
		it.localStatus.kvWriteSize.Add(int64(meta.Size))
	}

	return &kvapi.Meta{
		Version: cVer,
	}, nil
}

func (it *dbReplica) trySplit(shard, next *kvapi.DatabaseMap_Shard, avgSize int64) ([]byte, error) {

	rangeSize := func(lower, upper []byte) int64 {
		rs, err := it.store.SizeOf([]*storage.IterOptions{{
			LowerKey: lower,
			UpperKey: append(upper, 0x00),
		}})
		if err == nil && len(rs) > 0 {
			return rs[0]
		}
		return 0
	}

	var (
		startKey = keyEncode(nsKeyData, shard.LowerKey)

		leftKey  = bytesClone(startKey)
		rightKey []byte

		capRatio = float64(1.0)

		tn     = time.Now()
		tryNum = 0
	)

	if next != nil {
		rightKey = bytesRepeat(keyEncode(nsKeyData, next.LowerKey), 0xff, 48)
	} else {
		rightKey = bytesRepeat(keyEncode(nsKeyData, []byte{}), 0xff, 48)
	}

	var (
		repSize = float64(rangeSize(leftKey, rightKey))
		midSize = repSize * kShardSplit_CapacityThreshold
	)

	avgSize = avgSize << 20
	if d := absFloat64(repSize-float64(avgSize)) / float64(avgSize); d > 0.1 {

		hlog.Printf("info", "try shard split deny, size %d, avg-size %d, diff %.4f, time %v",
			int64(repSize)/(1<<20), avgSize/(1<<20), d, time.Since(tn))

		return nil, errors.New("size issue, try next ...")
	}

	for ; tryNum < 1024; tryNum++ {

		offset, ok := middleKey(leftKey, rightKey)
		if !ok {
			break
		}

		s := float64(rangeSize(startKey, offset))

		if s > repSize {
			repSize = s
			if repSize < 1024 {
				repSize = 1024
			}
			midSize = repSize * kShardSplit_CapacityThreshold
		}

		capRatio = absFloat64(s-midSize) / repSize

		if capRatio <= kShardSplit_CapacityBiasMin {
			leftKey = offset
			break
		} else if s <= midSize {
			leftKey = offset
		} else {
			rightKey = offset
		}
	}

	if capRatio > kShardSplit_CapacityBiasMax {

		testPrintf("miss s %v", startKey)
		testPrintf("miss l %v", leftKey)
		testPrintf("miss r %v", rightKey)

		return nil, fmt.Errorf("try split try %d fail %v", tryNum, capRatio)
	}

	testPrintf("try shard %d, split hit, key %v, size %d, size-ratio %f, cap-ratio %f, try-n %d, time %v",
		it.shardId, string(leftKey[1:]), int64(repSize)/(1<<20), midSize/repSize, capRatio, tryNum, time.Since(tn))

	hlog.Printf("info", "try shard split hit, key %v, size %f (%d/%d), cap-ratio %f, try-n %d, time %v",
		string(leftKey[1:]), midSize/repSize, int(midSize)/(1<<20), int(repSize)/(1<<20), capRatio, tryNum, time.Since(tn))

	if next != nil {
		hlog.Printf("info", "left %s, mid %s, right %s, shard %d %d",
			string(shard.LowerKey), string(leftKey[1:]), string(next.LowerKey), shard.Id, next.Id)
	} else {
		hlog.Printf("info", "left %s, mid %s, right %s, shard %d",
			string(shard.LowerKey), string(leftKey[1:]), "zzzz", shard.Id)
	}

	return leftKey[1:], nil
}
