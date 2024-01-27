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
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func (it *dbReplica) Write(req *kvapi.WriteRequest) *kvapi.ResultSet {
	return it.write(req, 0)
}

func (it *dbReplica) write(req *kvapi.WriteRequest, cLog uint64) *kvapi.ResultSet {

	var (
		t0 = time.Now()
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Write", time.Since(t0).Seconds())
		}()
	}

	if err := req.Valid(); err != nil {
		return newResultSetWithClientError(err.Error())
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	meta, err := it.getMeta(req.Key)
	if meta == nil && err != nil {
		return newResultSetWithServerError(err.Error())
	}

	if req.Meta == nil {
		req.Meta = &kvapi.Meta{}
	}

	var (
		updated = t0.UnixNano() / 1e6
	)

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
			return newResultSetWithClientError("invalid prev_incr_id")
		}

		if (cLog > 0 && meta.Version == cLog) ||
			req.CreateOnly ||
			(req.Meta.Expired == meta.Expired &&
				(req.Meta.IncrId == 0 || req.Meta.IncrId == meta.IncrId) &&
				(req.PrevIncrId == 0 || req.PrevIncrId == meta.IncrId) &&
				(req.Meta.Checksum > 0 && req.Meta.Checksum == meta.Checksum)) {

			rs := newResultSetOK()
			resultSetAppend(rs, req.Key, &kvapi.Meta{
				Version: meta.Version,
				IncrId:  meta.IncrId,
				Updated: meta.Updated,
				// Created: meta.Created,
			}, nil)
			return rs
		}

		if req.Meta.IncrId == 0 && meta.IncrId > 0 {
			req.Meta.IncrId = meta.IncrId
		}

		if meta.Attrs > 0 {
			req.Meta.Attrs |= meta.Attrs
		}

		// if meta.Created > 0 {
		// 	req.Meta.Created = meta.Created
		// }

		if len(meta.Extra) > 0 && len(req.Meta.Extra) == 0 {
			req.Meta.Extra = meta.Extra
		}
	}

	req.Meta.Updated = updated

	// if req.Meta.Created < 1 {
	// 	req.Meta.Created = updated
	// }

	if req.IncrNamespace != "" {

		if req.Meta.IncrId == 0 {
			req.Meta.IncrId, err = it.incrSync(req.IncrNamespace, 1, 0)
			if err != nil {
				return newResultSetWithServerError(err.Error())
			}
		} else {
			it.incrSync(req.IncrNamespace, 0, req.Meta.IncrId)
		}
	}

	cLogOn := true

	if false /* log-off */ {
		cLogOn = false
	} else {

		if cLog == 0 {
			if meta != nil && meta.Version > 0 {
				cLog = meta.Version
			}

			if cLog, err = it.versionSync(1, cLog); err != nil {
				return newResultSetWithServerError(err.Error())
			}
		} else {
			if _, err = it.versionSync(0, cLog); err != nil {
				return newResultSetWithServerError(err.Error())
			}
			cLogOn = false
		}
	}

	req.Meta.Version = cLog

	bsMeta, bsData, err := req.Encode()
	if err != nil {
		return newResultSetWithServerError(err.Error())
	}

	batch := it.store.NewBatch()

	writeSize := 0

	if !kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_IgnoreMeta) {
		batch.Put(keyEncode(nsKeyMeta, req.Key), bsMeta)
		writeSize += len(bsMeta)
	}
	if !kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_IgnoreData) {
		batch.Put(keyEncode(nsKeyData, req.Key), bsData)
		writeSize += len(bsData)
	}

	if (cLogOn && !it.cfg.Feature.WriteLogDisable) ||
		req.Meta.Expired > 0 {

		it.logMu.Lock()
		defer it.logMu.Unlock()

		logId, err := it.logSync(1, 0)
		if err != nil {
			return newResultSetWithServerError(err.Error())
		}

		bsLogMeta, err := req.LogEncode(logId, it.replicaId)
		if err != nil {
			return newResultSetWithServerError(err.Error())
		}

		if cLogOn && !it.cfg.Feature.WriteLogDisable {
			batch.Put(keyLogEncode(logId, it.replicaId, 0), bsLogMeta)
			// jsonPrint("log put", fmt.Sprintf("%d %d", it.replicaId, logId))
			writeSize += len(bsLogMeta)
		}

		if req.Meta.Expired > 0 {
			batch.Put(keyExpireEncode(req.Meta.Expired, req.Key), bsLogMeta)
			writeSize += len(bsLogMeta)
		}
	}

	if kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_Sync) {

		if ss := batch.Apply(&storage.WriteOptions{
			Sync: true,
		}); !ss.OK() {
			return newResultSetWithServerError(ss.ErrorMessage())
		}

	} else {
		if ss := batch.Apply(nil); !ss.OK() {
			return newResultSetWithServerError(ss.ErrorMessage())
		}
	}

	if it.cfg.Server.MetricsEnable {
		metricCounter.Add(metricStorage, "Key.Write", 1)
		metricCounter.Add(metricStorageSize, "Key.Write", float64(writeSize))
	}

	it.localStatus.kvWriteKeys.Add(1)
	it.localStatus.kvWriteSize.Add(int64(writeSize))

	rs := newResultSetOK()
	rs.MaxVersion = cLog
	resultSetAppend(rs, req.Key, &kvapi.Meta{
		Version: cLog,
		IncrId:  req.Meta.IncrId,
		Updated: req.Meta.Updated,
	}, nil)

	return rs
}

func (it *dbReplica) Delete(req *kvapi.DeleteRequest) *kvapi.ResultSet {

	var (
		t0 = time.Now()
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Delete", time.Since(t0).Seconds())
			metricCounter.Add(metricStorage, "Key.Delete", 1)
		}()
	}

	if err := req.Valid(); err != nil {
		return newResultSetWithClientError(err.Error())
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	meta, err := it.getMeta(req.Key)
	if meta == nil && err != nil {
		return newResultSetWithServerError(err.Error())
	} else if meta == nil {
		return newResultSetOK()
	}

	var (
		cLog    = uint64(0)
		updated = t0.UnixNano() / 1e6
	)

	if req.PrevVersion > 0 && req.PrevVersion != meta.Version {
		return newResultSetWithClientError("invalid prev_version")
	}

	if req.PrevChecksum > 0 && req.PrevChecksum != meta.Checksum {
		return newResultSetWithClientError("invalid prev_data_check")
	}

	if req.PrevAttrs > 0 && !kvapi.AttrAllow(meta.Attrs, req.PrevAttrs) {
		return newResultSetWithClientError("invalid prev_attrs")
	}

	cLogOn := true

	if false /* log-off */ {
		cLogOn = false
	} else {

		if cLog == 0 {
			if meta != nil && meta.Version > 0 {
				cLog = meta.Version
			}

			if cLog, err = it.versionSync(1, cLog); err != nil {
				return newResultSetWithServerError(err.Error())
			}
		} else {
			if _, err = it.versionSync(0, cLog); err != nil {
				return newResultSetWithServerError(err.Error())
			}
			cLogOn = false
		}
	}

	batch := it.store.NewBatch()

	if !kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_RetainMeta) {
		batch.Delete(keyEncode(nsKeyMeta, req.Key))
	}

	batch.Delete(keyEncode(nsKeyData, req.Key))

	if cLogOn && !it.cfg.Feature.WriteLogDisable {

		it.logMu.Lock()
		defer it.logMu.Unlock()

		logId, err := it.logSync(1, 0)
		if err != nil {
			return newResultSetWithServerError(err.Error())
		}

		bsLogMeta, err := req.LogEncode(logId, cLog, it.replicaId)
		if err != nil {
			return newResultSetWithServerError(err.Error())
		}

		batch.Put(keyLogEncode(logId, it.replicaId, 0), bsLogMeta)
	}

	if kvapi.AttrAllow(req.Attrs, kvapi.Write_Attrs_Sync) {

		if ss := batch.Apply(&storage.WriteOptions{
			Sync: true,
		}); !ss.OK() {
			return newResultSetWithServerError(ss.ErrorMessage())
		}
	} else {
		if ss := batch.Apply(nil); !ss.OK() {
			return newResultSetWithServerError(ss.ErrorMessage())
		}
	}

	it.localStatus.kvWriteKeys.Add(1)
	if meta != nil && meta.Size > 0 {
		it.localStatus.kvWriteSize.Add(int64(meta.Size))
	}

	rs := newResultSetOK()
	rs.MaxVersion = cLog

	resultSetAppend(rs, req.Key, &kvapi.Meta{
		Version: cLog,
		Updated: updated,
		Attrs:   req.Attrs,
	}, nil)

	return rs
}

func (it *dbReplica) Read(req *kvapi.ReadRequest) *kvapi.ResultSet {

	var (
		rs = newResultSetOK()
		t0 = time.Now()
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Read", time.Since(t0).Seconds())
		}()
	}

	// rs.MaxVersion, _ = it.versionSync(0, 0, 0)

	updated := t0.UnixNano() / 1e6

	for _, k := range req.Keys {

		var (
			ss storage.Result
		)

		if kvapi.AttrAllow(req.Attrs, kvapi.Read_Attrs_MetaOnly) {
			ss = it.store.Get(keyEncode(nsKeyMeta, k), nil)
		} else {
			ss = it.store.Get(keyEncode(nsKeyData, k), nil)
		}

		if it.cfg.Server.MetricsEnable {
			metricCounter.Add(metricStorage, "Key.Read", 1)
			metricCounter.Add(metricStorageSize, "Key.Read", float64(len(ss.Bytes())))
		}

		if ss.OK() {

			item, err := kvapi.KeyValueDecode(ss.Bytes())
			item.Key = k
			if err == nil {
				if item.Meta.Expired > 0 && item.Meta.Expired <= updated {
					if len(req.Keys) == 1 {
						rs.StatusCode = kvapi.Status_NotFound
					} else {
						rs.Items = append(rs.Items, &kvapi.KeyValue{
							Key:  k,
							Meta: &kvapi.Meta{},
						})
					}
				} else {
					rs.Items = append(rs.Items, item)
				}
			} else {
				rs.StatusCode, rs.StatusMessage = kvapi.Status_ServerError, err.Error()
			}

		} else {

			if !ss.NotFound() {
				rs.StatusCode, rs.StatusMessage = kvapi.Status_ServerError, ss.ErrorMessage()
				break
			}

			if len(req.Keys) == 1 {
				rs.StatusCode = kvapi.Status_NotFound
			}
		}
	}

	rs.MaxVersion, _ = it.versionSync(0, 0)

	return rs
}

func (it *dbReplica) Range(req *kvapi.RangeRequest) *kvapi.ResultSet {

	var (
		t0       = time.Now()
		tms      = t0.UnixNano() / 1e6
		rs       = newResultSetOK()
		readSize int
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Range", time.Since(t0).Seconds())
			metricCounter.Add(metricStorage, "Key.Range", 1)
			metricCounter.Add(metricStorage, "Key.Range.Item", float64(len(rs.Items)))
			metricCounter.Add(metricStorageSize, "Key.Range", float64(readSize))
		}()
	}

	nsKey := nsKeyData
	if kvapi.AttrAllow(req.Attrs, kvapi.Read_Attrs_MetaOnly) {
		nsKey = nsKeyMeta
	}

	var (
		lowerKey  = keyEncode(nsKey, bytesClone(req.LowerKey))
		upperKey  = keyEncode(nsKey, bytesClone(req.UpperKey))
		limitNum  = int(req.Limit)
		limitSize = 4 << 20
	)

	if limitNum > kvapi.Service_Range_MaxLimit {
		limitNum = kvapi.Service_Range_MaxLimit
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = kvapi.Service_Range_DefSize
	} else if limitSize > kvapi.Service_Range_MaxSize {
		limitSize = kvapi.Service_Range_MaxSize
	}

	if req.Revert {
		// (upper, lower]
		// lowerKey = append(lowerKey, 0xff)
		// upperKey = append(upperKey, 0x00)
	} else {
		// (lower, upper]
		lowerKey = append(lowerKey, 0x00)
		upperKey = append(upperKey, 0xff)
	}

	var (
		ok bool
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: lowerKey,
		UpperKey: upperKey,
	})
	if err != nil {
		rs.StatusCode = kvapi.Status_InvalidArgument
		rs.StatusMessage = err.Error()
		return rs
	}

	defer iter.Release()

	move := func() bool {
		if req.Revert {
			return iter.Prev()
		}
		return iter.Next()
	}

	if req.Revert {
		ok = iter.SeekToLast()
	} else {
		ok = iter.SeekToFirst()
	}

	for ; ok; ok = move() {

		if req.Revert {
			if bytes.Compare(iter.Key(), upperKey) >= 0 {
				continue
			}
		}

		item, err := kvapi.KeyValueDecode(bytesClone(iter.Value()))
		if err != nil {
			continue
		}

		if item.Meta.Expired > 0 && item.Meta.Expired <= tms {
			continue
		}

		if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
			rs.NextResultSet = true
			break
		}
		readSize += len(iter.Value())

		item.Key = bytesClone(iter.Key())[1:]
		rs.Items = append(rs.Items, item)

		if len(rs.Items) >= limitNum || readSize >= limitSize {
			rs.NextResultSet = true
			break
		}
	}

	if len(rs.Items) == 0 {
		rs.StatusCode = kvapi.Status_NotFound
	}

	rs.MaxVersion, _ = it.versionSync(0, 0)

	return rs
}

func (it *dbReplica) Batch(breq *kvapi.BatchRequest) *kvapi.BatchResponse {

	var (
		t0  = time.Now()
		brs = &kvapi.BatchResponse{}
	)

	if it.cfg.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Batch", time.Since(t0).Seconds())
			metricCounter.Add(metricStorage, "Batch", 1)
		}()
	}

	breq.Database = it.dbName
	if err := breq.Valid(); err != nil {
		brs.StatusCode, brs.StatusMessage = kvapi.Status_InvalidArgument, err.Error()
		return brs
	}

	ok := 0

	for _, req := range breq.Items {
		var rs *kvapi.ResultSet
		switch req.Value.(type) {
		case *kvapi.RequestUnion_Write:
			rs = it.Write(req.Value.(*kvapi.RequestUnion_Write).Write)

		case *kvapi.RequestUnion_Delete:
			rs = it.Delete(req.Value.(*kvapi.RequestUnion_Delete).Delete)

		case *kvapi.RequestUnion_Read:
			rs = it.Read(req.Value.(*kvapi.RequestUnion_Read).Read)

		case *kvapi.RequestUnion_Range:
			rs = it.Range(req.Value.(*kvapi.RequestUnion_Range).Range)

		default:
			rs = newResultSetWithClientError("invalid request type #replica-api")
		}
		brs.Items = append(brs.Items, rs)
		if rs.OK() {
			ok += 1
		} else if brs.StatusMessage == "" {
			brs.StatusCode = rs.StatusCode
			brs.StatusMessage = rs.StatusMessage
		}
	}

	if ok == len(breq.Items) {
		brs.StatusCode = kvapi.Status_OK
	}

	return brs
}

// debug api
func (it *dbReplica) RawRange(req *kvapi.RangeRequest) ([]*kvapi.RawKeyValue, error) {

	var (
		lowerKey  = bytesClone(req.LowerKey)
		upperKey  = bytesClone(req.UpperKey)
		limitNum  = int(req.Limit)
		readSize  int
		limitSize = 4 << 20
		items     []*kvapi.RawKeyValue
	)

	if limitNum > kvapi.Service_Range_MaxLimit {
		limitNum = kvapi.Service_Range_MaxLimit
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = kvapi.Service_Range_DefSize
	} else if limitSize > kvapi.Service_Range_MaxSize {
		limitSize = kvapi.Service_Range_MaxSize
	}

	if req.Revert {
		// (upper, lower]
		// lowerKey = append(lowerKey, 0xff)
		// upperKey = append(upperKey, 0x00)
	} else {
		// (lower, upper]
		lowerKey = append(lowerKey, 0x00)
		upperKey = append(upperKey, 0xff)
	}

	var (
		ok bool
	)

	iter, err := it.store.NewIterator(&storage.IterOptions{
		LowerKey: lowerKey,
		UpperKey: upperKey,
	})
	if err != nil {
		return nil, err
	}

	defer iter.Release()

	move := func() bool {
		if req.Revert {
			return iter.Prev()
		}
		return iter.Next()
	}

	if req.Revert {
		ok = iter.SeekToLast()
	} else {
		ok = iter.SeekToFirst()
	}

	for ; ok; ok = move() {

		if req.Revert {
			if bytes.Compare(iter.Key(), upperKey) >= 0 {
				continue
			}
		}

		if len(items) > 0 && readSize+len(iter.Value()) > limitSize {
			break
		}
		readSize += len(iter.Value())

		item := &kvapi.RawKeyValue{
			Key:   bytesClone(iter.Key()),
			Value: bytesClone(iter.Value()),
		}
		items = append(items, item)

		if len(items) >= limitNum || readSize >= limitSize {
			break
		}
	}

	return items, nil
}

func (it *dbReplica) SetDatabase(name string) kvapi.Client {
	return it
}
