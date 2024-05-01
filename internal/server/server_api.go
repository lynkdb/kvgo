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
	mrand "math/rand"
	"time"

	// "github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type pQueItem struct {
	logVersion uint64
	incrId     uint64
}

func (it *dbServer) apiWrite(req *kvapi.WriteRequest, selectShard *dbMapSelectShard) *kvapi.ResultSet {

	if selectShard == nil {
		return newResultSetWithServerError("server not ready : shard init")
	}

	if selectShard.replicaNum < minReplicaCap ||
		selectShard.replicaNum > maxReplicaCap ||
		len(selectShard.replicas)*2 < selectShard.replicaNum {

		return newResultSetWithServerError("server not ready : replicas %d/%d",
			len(selectShard.replicas), selectShard.replicaNum)
	}

	var (
		t0 = timeNow()
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricCounter.Add(metricService, "Key.Write", 1)
			metricLatency.Add(metricService, "Key.Write", time.Since(t0).Seconds())
			metricCounter.Add(metricServiceSize, "Key.Write", float64(len(req.Key)+len(req.Value)))
		}()
	}

	if len(selectShard.replicas) < selectShard.replicaNum {
		testPrintf("write to replicas %d/%d", len(selectShard.replicas), selectShard.replicaNum)
	}

	mainReplica := selectShard.replicas[0]

	meta, err := mainReplica.getMeta(req.Key)
	if meta == nil && err != nil {
		return newResultSetWithServerError(err.Error())
	}

	if req.Meta == nil {
		req.Meta = &kvapi.Meta{}
	}

	if req.Meta.Updated <= 0 || !kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_InnerSync) {
		req.Meta.Updated = t0.UnixNano() / 1e6
	}

	if meta != nil {

		if req.PrevVersion > 0 && req.PrevVersion != meta.Version {
			return newResultSetWithClientError("invalid prev_version")
		}

		if req.PrevChecksum > 0 && req.PrevChecksum != meta.Checksum {
			return newResultSetWithClientError("invalid prev_data_check")
		}

		if req.PrevAttrs > 0 && !kvapi.AttrAllow(meta.Attrs, req.PrevAttrs) {
			return newResultSetWithClientError("invalid prev_attrs")
		}

		if req.PrevIncrId > 0 && req.PrevIncrId != meta.IncrId {
			return newResultSetWithClientError("invalid prev_incr_id (prev %d, req %d)", meta.IncrId, req.PrevIncrId)
		}

		if req.CreateOnly ||
			(req.Meta.Updated < meta.Updated) ||
			(req.Meta.Expired == meta.Expired &&
				(req.Meta.IncrId == 0 || req.Meta.IncrId == meta.IncrId) &&
				(req.PrevIncrId == 0 || req.PrevIncrId == meta.IncrId) &&
				(req.Meta.Checksum > 0 && req.Meta.Checksum == meta.Checksum)) {

			rs := newResultSetOK()
			resultSetAppend(rs, req.Key, &kvapi.Meta{
				Version: meta.Version,
				IncrNs:  meta.IncrNs,
				IncrId:  meta.IncrId,
				Updated: meta.Updated,
				// Created: meta.Created,
			}, nil)
			return rs
		}

		if req.Meta.IncrNs == 0 && meta.IncrNs > 0 {
			req.Meta.IncrNs = meta.IncrNs
		}

		if req.Meta.IncrId == 0 && meta.IncrId > 0 {
			req.Meta.IncrId = meta.IncrId
		}

		if meta.Attrs > 0 && !kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_InnerSync) {
			req.Meta.Attrs |= meta.Attrs
		}

		// if meta.Created > 0 {
		// 	req.Meta.Created = meta.Created
		// }

		if len(meta.Extra) > 0 && len(req.Meta.Extra) == 0 {
			req.Meta.Extra = meta.Extra
		}
	}

	// if req.Meta.Created == 0 {
	// 	req.Meta.Created = req.Meta.Updated
	// }

	var (
		nCap = selectShard.replicaNum
		pNum = 0
		pLog = uint64(0)
		pInc = uint64(0)
		pQue = make(chan pQueItem, nCap+1)
		pTTL = time.Millisecond * time.Duration(writeProposalTTL)
	)

	reqPrepare := &kvapi.WriteProposalRequest{
		Id:    randUint64(),
		Write: req,
	}

	for _, trep := range selectShard.replicas {

		go func(trep *dbReplica, req *kvapi.WriteProposalRequest) {

			pMeta, err := trep._writePrepare(req)

			if err == nil && pMeta != nil && pMeta.Version > 0 {
				pQue <- pQueItem{
					logVersion: pMeta.Version,
					incrId:     pMeta.IncrId,
				}
			} else {
				pQue <- pQueItem{
					logVersion: 0,
				}
			}
		}(trep, reqPrepare)
	}

	for {

		select {
		case v := <-pQue:
			if v.logVersion > 0 {
				pNum += 1
				if v.logVersion > pLog {
					pLog = v.logVersion
				}
				if v.incrId > pInc {
					pInc = v.incrId
				}
			}

		case <-time.After(pTTL):
			pTTL = -1
		}

		if (pNum*2) > nCap || pTTL == -1 {
			if pNum < nCap && pTTL > 0 {
				pTTL = time.Millisecond * 10
				continue
			}
			break
		}
	}

	if (pNum * 2) <= nCap {
		return newResultSetWithServerError("p1 fail %d/%d", pNum, nCap)
	}

	pNum = 0
	pTTL = time.Millisecond * time.Duration(writeProposalTTL)
	pQue2 := make(chan uint64, nCap+1)

	reqAccept := &kvapi.WriteProposalRequest{
		Id: reqPrepare.Id,
		Write: &kvapi.WriteRequest{
			Key: req.Key,
			Meta: &kvapi.Meta{
				Version: pLog,
				IncrId:  pInc,
			},
		},
	}

	for _, trep := range selectShard.replicas {
		go func(trep *dbReplica, req *kvapi.WriteProposalRequest) {

			aMeta, err := trep._writeAccept(req)

			if err == nil && aMeta != nil && aMeta.Version == pLog {
				pQue2 <- 1
			} else {
				pQue2 <- 0
			}

		}(trep, reqAccept)
	}

	for {

		select {
		case v := <-pQue2:
			if v == 1 {
				pNum += 1
			}

		case <-time.After(pTTL):
			pTTL = -1
		}

		if (pNum*2) > nCap || pTTL == -1 {
			if pNum < nCap && pTTL > 0 {
				pTTL = time.Millisecond * 10
				continue
			}
			break
		}
	}

	if (pNum * 2) <= nCap {
		return newResultSetWithServerError("p2 fail %d/%d", pNum, nCap)
	}

	rs := newResultSetOK()
	resultSetAppend(rs, req.Key, &kvapi.Meta{
		Version: pLog,
		IncrId:  pInc,
		Updated: req.Meta.Updated,
	}, nil)

	return rs
}

func (it *dbServer) apiDelete(req *kvapi.DeleteRequest, selectShard *dbMapSelectShard) *kvapi.ResultSet {

	if selectShard == nil {
		return newResultSetWithServerError("server not ready : shard init")
	}

	if selectShard.replicaNum < minReplicaCap ||
		selectShard.replicaNum > maxReplicaCap ||
		len(selectShard.replicas)*2 < selectShard.replicaNum {

		return newResultSetWithServerError("server not ready : replicas %d/%d",
			len(selectShard.replicas), selectShard.replicaNum)
	}

	var (
		t0 = timeNow()
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricCounter.Add(metricService, "Key.Delete", 1)
			metricLatency.Add(metricService, "Key.Delete", time.Since(t0).Seconds())
			metricCounter.Add(metricServiceSize, "Key.Delete", float64(len(req.Key)))
		}()
	}

	mainReplica := selectShard.replicas[0]

	meta, err := mainReplica.getMeta(req.Key)
	if meta == nil && err != nil {
		return newResultSetWithServerError(err.Error())
	}

	if meta == nil {
		return newResultSetOK()
	}

	if meta != nil {

		if req.PrevVersion > 0 && req.PrevVersion != meta.Version {
			return newResultSetWithClientError("invalid prev_version")
		}

		if req.PrevChecksum > 0 && req.PrevChecksum != meta.Checksum {
			return newResultSetWithClientError("invalid prev_data_check")
		}

		if req.PrevAttrs > 0 && !kvapi.AttrAllow(meta.Attrs, req.PrevAttrs) {
			return newResultSetWithClientError("invalid prev_attrs")
		}
	}

	var (
		nCap = selectShard.replicaNum
		pNum = 0
		pLog = uint64(0)
		pInc = uint64(0)
		pQue = make(chan pQueItem, nCap+1)
		pTTL = time.Millisecond * time.Duration(writeProposalTTL)
	)

	reqPrepare := &kvapi.DeleteProposalRequest{
		Id:    randUint64(),
		Key:   req.Key,
		Meta:  &kvapi.Meta{},
		Attrs: req.Attrs,
	}

	for _, trep := range selectShard.replicas {

		go func(trep *dbReplica, req *kvapi.DeleteProposalRequest) {

			pMeta, err := trep._deletePrepare(req)

			if err == nil && pMeta != nil && pMeta.Version > 0 {
				pQue <- pQueItem{
					logVersion: pMeta.Version,
					incrId:     pMeta.IncrId,
				}
			} else {
				pQue <- pQueItem{
					logVersion: 0,
				}
			}
		}(trep, reqPrepare)
	}

	for {

		select {
		case v := <-pQue:
			if v.logVersion > 0 {
				pNum += 1
				if v.logVersion > pLog {
					pLog = v.logVersion
				}
				if v.incrId > pInc {
					pInc = v.incrId
				}
			}

		case <-time.After(pTTL):
			pTTL = -1
		}

		if (pNum*2) > nCap || pTTL == -1 {
			if pNum < nCap && pTTL > 0 {
				pTTL = time.Millisecond * 10
				continue
			}
			break
		}
	}

	if (pNum * 2) <= nCap {
		return newResultSetWithServerError("p1 fail %d/%d", pNum, nCap)
	}

	pNum = 0
	pTTL = time.Millisecond * time.Duration(writeProposalTTL)
	pQue2 := make(chan uint64, nCap+1)

	reqAccept := &kvapi.DeleteProposalRequest{
		Id:  reqPrepare.Id,
		Key: req.Key,
		Meta: &kvapi.Meta{
			Version: pLog,
		},
		Attrs: req.Attrs,
	}

	for _, trep := range selectShard.replicas {
		go func(trep *dbReplica, req *kvapi.DeleteProposalRequest) {

			aMeta, err := trep._deleteAccept(req)

			if err == nil && aMeta != nil && aMeta.Version == pLog {
				pQue2 <- 1
			} else {
				pQue2 <- 0
			}

		}(trep, reqAccept)
	}

	for {

		select {
		case v := <-pQue2:
			if v == 1 {
				pNum += 1
			}

		case <-time.After(pTTL):
			pTTL = -1
		}

		if (pNum*2) > nCap || pTTL == -1 {
			if pNum < nCap && pTTL > 0 {
				pTTL = time.Millisecond * 10
				continue
			}
			break
		}
	}

	if (pNum * 2) <= nCap {
		return newResultSetWithServerError("p2 fail %d/%d", pNum, nCap)
	}

	rs := newResultSetOK()
	resultSetAppend(rs, req.Key, &kvapi.Meta{
		Version: pLog,
	}, nil)

	return rs
}

func (it *dbServer) apiReadShard(req *kvapi.ReadRequest, selectShard *dbMapSelectShard) *kvapi.ResultSet {
	if selectShard == nil {
		return newResultSetWithServerError("server not ready : shard init")
	}

	if selectShard.replicaNum < minReplicaCap ||
		selectShard.replicaNum > maxReplicaCap ||
		len(selectShard.replicas)*2 < selectShard.replicaNum {

		return newResultSetWithServerError("server not ready : replicas %d/%d",
			len(selectShard.replicas), selectShard.replicaNum)
	}

	if len(selectShard.replicas) > 1 {
		if i := mrand.Intn(len(selectShard.replicas)); i > 0 {
			selectShard.replicas = append(selectShard.replicas[i:], selectShard.replicas[:i]...)
		}
	}

	var rs *kvapi.ResultSet

	for _, rep := range selectShard.replicas {

		rs = rep.Read(&kvapi.ReadRequest{
			Keys:  selectShard.keys,
			Attrs: req.Attrs,
		})

		if rs.OK() || rs.NotFound() {
			break
		}
	}

	return rs
}

func (it *dbServer) apiRead(req *kvapi.ReadRequest, selectShards []*dbMapSelectShard) *kvapi.ResultSet {

	if len(selectShards) == 0 {
		return newResultSetWithServerError("server not ready : shard init")
	}

	var (
		t0 = timeNow()
		sz int
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricCounter.Add(metricService, "Key.Read", 1)
			metricLatency.Add(metricService, "Key.Read", time.Since(t0).Seconds())
			metricCounter.Add(metricServiceSize, "Key.Read", float64(sz))
		}()
	}

	var (
		indexes = map[string][]int{}
		rs      = &kvapi.ResultSet{
			Items: make([]*kvapi.KeyValue, len(req.Keys)),
		}
		hit = 0
	)

	for i, key := range req.Keys {
		if _, ok := indexes[string(key)]; ok {
			indexes[string(key)] = append(indexes[string(key)], i)
		} else {
			indexes[string(key)] = []int{i}
		}
	}

	for _, selectShard := range selectShards {
		rs2 := it.apiReadShard(req, selectShard)
		for _, item := range rs2.Items {
			if idx, ok := indexes[string(item.Key)]; ok && len(idx) > 0 {
				rs.Items[idx[0]] = item
				indexes[string(item.Key)] = idx[1:]
				hit += 1
				sz += len(item.Key) + len(item.Value)
			}
		}
		if rs2.MaxVersion > rs.MaxVersion {
			rs.MaxVersion = rs2.MaxVersion
		}
	}

	if hit == len(req.Keys) {
		rs.StatusCode = kvapi.Status_OK
	} else {
		rs.StatusCode = kvapi.Status_NotFound
	}

	return rs
}

func (it *dbServer) apiRange(req *kvapi.RangeRequest, selectShards []*dbMapSelectShard) *kvapi.ResultSet {

	if len(selectShards) == 0 {
		return newResultSetWithServerError("server not ready : shard init")
	}

	var (
		t0 = timeNow()
		sz int
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricCounter.Add(metricService, "Key.Range", 1)
			metricLatency.Add(metricService, "Key.Range", time.Since(t0).Seconds())
			metricCounter.Add(metricServiceSize, "Key.Range", float64(sz))
		}()
	}

	var (
		rs    = &kvapi.ResultSet{}
		limit = req.Limit
	)

	for _, selectShard := range selectShards {
		if len(selectShard.replicas) == 0 {
			return newResultSetWithServerError("shard in pending")
		}
		if bytes.Compare(selectShard.lowerKey, req.LowerKey) < 0 {
			selectShard.lowerKey = bytesClone(req.LowerKey)
		}
		if len(selectShard.upperKey) == 0 || bytes.Compare(selectShard.upperKey, req.UpperKey) > 0 {
			selectShard.upperKey = bytesClone(req.UpperKey)
		}
		rs2 := selectShard.replicas[0].Range(&kvapi.RangeRequest{
			LowerKey: selectShard.lowerKey,
			UpperKey: selectShard.upperKey,
			Attrs:    req.Attrs,
			Revert:   req.Revert,
			Limit:    limit,
		})
		if !rs2.OK() && !rs2.NotFound() {
			return rs
		}
		if len(rs2.Items) > 0 {
			for _, item := range rs2.Items {
				sz += len(item.Key) + len(item.Value)
			}
			rs.Items = append(rs.Items, rs2.Items...)
			limit -= int64(len(rs2.Items))
			if limit <= 0 {
				break
			}
		}
	}

	if len(rs.Items) == 0 {
		rs.StatusCode = kvapi.Status_NotFound
	} else {
		rs.StatusCode = kvapi.Status_OK
	}

	return rs
}

type innerLogRangeFilter struct {
	index int
	size  int
	meta  *kvapi.LogMeta
}

func (it *dbServer) apiInnerLogRange(req *kvapi.LogRangeRequest,
	dm *dbMap, token *logRangeToken) *kvapi.LogRangeResponse {

	var (
		t0        = timeNow()
		sz        int
		limitNum  = 10000
		limitSize = 2 << 20
		readSize  int
		rs        = &kvapi.LogRangeResponse{
			ServerId:   it.cfg.Server.ID,
			DatabaseId: dm.data.Id,
		}
		keyLogMap = map[string]*innerLogRangeFilter{}
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricCounter.Add(metricService, "Log.Range", 1)
			metricLatency.Add(metricService, "Log.Range", time.Since(t0).Seconds())
			metricCounter.Add(metricServiceSize, "Log.Range", float64(sz))
			metricCounter.Add(metricService, "Log.RangeN", float64(len(rs.Items)))
		}()
	}

	storeIter := func(store storage.Conn, tokenItem *logRangeTokenItem, rs *kvapi.LogRangeResponse) error {

		var (
			lowerKey = keyLogEncode(tokenItem.offset+1, 0, 0)
			upperKey = keyLogEncode(1<<63, 0, 0)
			hitNum   = 0
		)

		iter, err := store.NewIterator(&storage.IterOptions{
			LowerKey: lowerKey,
			UpperKey: upperKey,
		})
		if err != nil {
			return err
		}
		defer iter.Release()

		for ok := iter.SeekToFirst(); ok; ok = iter.Next() {

			if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
				break
			}

			logMeta, err := kvapi.LogDecode(bytesClone(iter.Value()))
			if err != nil {
				continue
			}

			if hitNum == 0 && logMeta.Id > (tokenItem.offset+1) {
				rs.LogOffsetOutrange = true
				// hlog.Printf("info", "out range")
				break
			}

			if logMeta.Id <= tokenItem.offset {
				continue
			}

			tokenItem.offset = logMeta.Id

			readSize += len(iter.Value())
			hitNum += 1

			if p, ok := keyLogMap[string(logMeta.Key)]; ok {

				if logMeta.Version > p.meta.Version {

					readSize -= p.size
					p.size = len(iter.Value())
					p.meta = logMeta
					rs.Items[p.index] = logMeta
				}

			} else {
				keyLogMap[string(logMeta.Key)] = &innerLogRangeFilter{
					index: len(rs.Items),
					size:  len(iter.Value()),
					meta:  logMeta,
				}
				rs.Items = append(rs.Items, logMeta)
			}

			if len(rs.Items) >= limitNum || readSize >= limitSize {
				break
			}
		}

		return nil
	}

	for _, v := range token.nextIndex {
		store := it.storeMgr.store(v.store)
		if store == nil {
			rs.Status = newServiceStatus(kvapi.Status_ServerError, "store not setup")
			break
		}

		if err := storeIter(store, v, rs); err != nil {
			rs.Status = newServiceStatus(kvapi.Status_ServerError, err.Error())
			break
		}

		if len(rs.Items) >= limitNum || readSize >= limitSize || rs.LogOffsetOutrange {
			break
		}
	}

	if rs.LogOffsetOutrange {

		storeLogOffsets, err := dm.lookupAllStoreLogOffsets()
		if err != nil {
			rs.Status = newServiceStatus(kvapi.Status_ServerError, "")
			return rs
		}

		logToken := newLogRangeToken("")
		logToken.indexApply(storeLogOffsets)

		rs.NextOffsetToken = logToken.encode()
		// hlog.Printf("info", "log out-range with token %v", logToken.data1)
	} else {
		rs.NextOffsetToken = token.encode()
		// hlog.Printf("info", "log next token %v", token.data1)
	}

	if len(rs.Items) == 0 && !rs.LogOffsetOutrange {
		rs.Status = newServiceStatus(kvapi.Status_NotFound, "")
	} else {
		rs.Status = newServiceStatus(kvapi.Status_OK, "")
	}

	return rs
}
