// Copyright 2015 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
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

package kvgo

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/hooto/hlog4g/hlog"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

func (cn *Conn) Commit(rr *kv2.ObjectWriter) *kv2.ObjectResult {

	if len(cn.opts.Cluster.MainNodes) > 0 {

		if cn.opts.ClientConnectEnable {
			return cn.objectCommitRemote(rr, 0)
		}

		rs, err := cn.public.Commit(nil, rr)
		if err != nil {
			return kv2.NewObjectResultServerError(err)
		}
		return rs
	}

	return cn.commitLocal(rr, 0)
}

func (cn *Conn) commitLocal(rr *kv2.ObjectWriter, cLog uint64) *kv2.ObjectResult {

	if err := rr.CommitValid(); err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	t0 := time.Now()
	if cn.opts.Server.MetricsEnable {
		defer func() {
			if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDelete) {
				metricLatency.Add(metricStorage, "Key.Delete", time.Since(t0).Seconds())
			} else {
				metricLatency.Add(metricStorage, "Key.Write", time.Since(t0).Seconds())
			}
		}()
	}

	if cn.monitor != nil {
		tp := timeus()
		defer func() {
			cn.monitor.Metric(MetricStorageLatency).With(map[string]string{
				"Write": "Key",
			}).Add(timeus() - tp)
		}()
	}

	cn.mu.Lock()
	defer cn.mu.Unlock()

	meta, err := cn.objectMetaGet(rr)
	if meta == nil && err != nil {
		return kv2.NewObjectResultServerError(err)
	}

	tdb := cn.tabledb(rr.TableName)
	if tdb == nil {
		return kv2.NewObjectResultClientError(errors.New("table not found"))
	}

	if meta == nil {

		if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDelete) {

			return kv2.NewObjectResultOK()
		}

	} else {

		if rr.PrevVersion > 0 && rr.PrevVersion != meta.Version {
			return kv2.NewObjectResultClientError(errors.New("invalid prev_version"))
		}

		if rr.PrevDataCheck > 0 && rr.PrevDataCheck != meta.DataCheck {
			return kv2.NewObjectResultClientError(errors.New("invalid prev_data_check"))
		}

		if rr.PrevAttrs > 0 && !kv2.AttrAllow(meta.Attrs, rr.PrevAttrs) {
			return kv2.NewObjectResultClientError(errors.New("invalid prev_attrs"))
		}

		if rr.PrevIncrId > 0 && rr.PrevIncrId != meta.IncrId {
			return kv2.NewObjectResultClientError(errors.New("invalid prev_incr_id"))
		}

		if (cLog > 0 && meta.Version == cLog) ||
			kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeCreate) ||
			(rr.Meta.Updated < meta.Updated) ||
			(rr.Meta.Expired == meta.Expired &&
				(rr.Meta.IncrId == 0 || rr.Meta.IncrId == meta.IncrId) &&
				(rr.PrevIncrId == 0 || rr.PrevIncrId == meta.IncrId) &&
				rr.Meta.DataCheck == meta.DataCheck) {

			rs := kv2.NewObjectResultOK()
			rs.Meta = &kv2.ObjectMeta{
				Version: meta.Version,
				IncrId:  meta.IncrId,
				Created: meta.Created,
				Updated: meta.Updated,
			}
			return rs
		}

		if rr.Meta.IncrId == 0 && meta.IncrId > 0 {
			rr.Meta.IncrId = meta.IncrId
		}

		if meta.Attrs > 0 {
			rr.Meta.Attrs |= meta.Attrs
		}

		if meta.Created > 0 {
			rr.Meta.Created = meta.Created
		}

		if len(meta.Extra) > 0 && len(rr.Meta.Extra) == 0 {
			rr.Meta.Extra = meta.Extra
		}
	}

	updated := uint64(time.Now().UnixNano() / 1e6)

	rr.Meta.Updated = updated

	if rr.Meta.Created < 1 {
		rr.Meta.Created = updated
	}

	if rr.IncrNamespace != "" {

		if rr.Meta.IncrId == 0 {
			rr.Meta.IncrId, err = tdb.objectIncrSet(rr.IncrNamespace, 1, 0)
			if err != nil {
				return kv2.NewObjectResultServerError(err)
			}
		} else {
			tdb.objectIncrSet(rr.IncrNamespace, 0, rr.Meta.IncrId)
		}
	}

	cLogOn := true

	if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeLogOff) {
		cLogOn = false
	} else {

		if cLog == 0 {
			if meta != nil && meta.Version > 0 {
				cLog = meta.Version
			}

			cLog, err = tdb.objectLogVersionSet(1, cLog, updated)
			if err != nil {
				return kv2.NewObjectResultServerError(err)
			}
		} else {
			_, err = tdb.objectLogVersionSet(0, cLog, updated)
			if err != nil {
				return kv2.NewObjectResultServerError(err)
			}
			cLogOn = false
		}
	}
	rr.Meta.Version = cLog

	if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDelete) {

		if cn.opts.Server.MetricsEnable {
			metricCounter.Add(metricStorage, "Key.Delete", 1)
		}

		if cn.monitor != nil {
			cn.monitor.Metric(MetricStorageCall).With(map[string]string{
				"Write": "Key",
			}).Inc(1)
		}

		if !kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDeleteDataOnly) {
			rr.Meta.Attrs |= kv2.ObjectMetaAttrDelete
		}

		if bsMeta, err := rr.MetaEncode(); err == nil {

			batch := tdb.db.NewBatch()

			if meta != nil {
				if !kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDeleteDataOnly) {
					batch.Delete(keyEncode(nsKeyMeta, rr.Meta.Key))
					batch.Delete(keyEncode(nsKeyData, rr.Meta.Key))
					batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
				} else {
					batch.Delete(keyEncode(nsKeyData, rr.Meta.Key))
				}
			}

			if cLogOn && !cn.opts.Feature.WriteLogDisable &&
				!kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDeleteDataOnly) {
				batch.Put(keyEncode(nsKeyLog, uint64ToBytes(cLog)), bsMeta)
				tdb.logSyncBuffer.put(cLog, rr.Meta.Attrs, rr.Meta.Key, true)
			}

			err = batch.Commit()
		}

	} else {

		if bsMeta, bsData, err := rr.PutEncode(); err == nil {

			batch := tdb.db.NewBatch()

			if kv2.AttrAllow(rr.Meta.Attrs, kv2.ObjectMetaAttrDataOff) {
				batch.Put(keyEncode(nsKeyMeta, rr.Meta.Key), bsData)
				batch.Delete(keyEncode(nsKeyData, rr.Meta.Key))
			} else if kv2.AttrAllow(rr.Meta.Attrs, kv2.ObjectMetaAttrMetaOff) {
				batch.Delete(keyEncode(nsKeyMeta, rr.Meta.Key))
				batch.Put(keyEncode(nsKeyData, rr.Meta.Key), bsData)
			} else {
				batch.Put(keyEncode(nsKeyMeta, rr.Meta.Key), bsMeta)
				batch.Put(keyEncode(nsKeyData, rr.Meta.Key), bsData)
			}

			if cn.opts.Server.MetricsEnable {
				metricCounter.Add(metricStorage, "Key.Write", 1)
				metricCounter.Add(metricStorageSize, "Key.Write", float64(len(bsMeta)+len(bsData)))
			}

			if cn.monitor != nil {
				cn.monitor.Metric(MetricStorageCall).With(map[string]string{
					"Write": "Key",
				}).Add(int64(len(bsMeta) + len(bsData)))
			}

			if cLogOn && !cn.opts.Feature.WriteLogDisable {
				batch.Put(keyEncode(nsKeyLog, uint64ToBytes(cLog)), bsMeta)
				if cn.opts.Server.MetricsEnable {
					metricCounter.Add(metricStorageSize, "Key.Write", float64(len(bsMeta)))
				}
			}

			if rr.Meta.Expired > 0 {
				batch.Put(keyExpireEncode(nsKeyTtl, rr.Meta.Expired, rr.Meta.Key), bsMeta)
				if cn.opts.Server.MetricsEnable {
					metricCounter.Add(metricStorageSize, "Key.Write", float64(len(bsMeta)))
				}
			}

			if meta != nil {
				if meta.Version < cLog && !cn.opts.Feature.WriteLogDisable {
					batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
				}
				if meta.Expired > 0 && meta.Expired != rr.Meta.Expired {
					batch.Delete(keyExpireEncode(nsKeyTtl, meta.Expired, rr.Meta.Key))
				}
			}

			err = batch.Commit()

			if err == nil && cLogOn {
				tdb.logSyncBuffer.put(cLog, rr.Meta.Attrs, rr.Meta.Key, true)
				tdb.objectLogFree(cLog)
			}
		}
	}

	if err != nil {
		return kv2.NewObjectResultServerError(err)
	}

	rs := kv2.NewObjectResultOK()
	rs.Meta = &kv2.ObjectMeta{
		Version: cLog,
		IncrId:  rr.Meta.IncrId,
		Updated: rr.Meta.Updated,
	}

	return rs
}

func (cn *Conn) objectCommitRemote(rr *kv2.ObjectWriter, cLog uint64) *kv2.ObjectResult {

	err := rr.CommitValid()
	if err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	mainNodes := cn.opts.Cluster.randMainNodes(3)
	if len(mainNodes) < 1 {
		return kv2.NewObjectResultClientError(errors.New("no master found"))
	}

	for _, v := range mainNodes {

		conn, err := clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, false)
		if err != nil {
			continue
		}

		ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
		defer fc()

		rs, err := kv2.NewPublicClient(conn).Commit(ctx, rr)
		if err != nil {
			return kv2.NewObjectResultServerError(err)
		}

		return rs
	}

	return kv2.NewObjectResultServerError(errors.New("no cluster nodes"))
}

func (cn *Conn) Query(rr *kv2.ObjectReader) *kv2.ObjectResult {

	if cn.opts.ClientConnectEnable ||
		(len(cn.opts.Cluster.MainNodes) > 0 && cn.opts.Server.Bind == "") {
		return cn.objectQueryRemote(rr)
	}

	return cn.objectLocalQuery(rr)
}

func (cn *Conn) objectLocalQuery(rr *kv2.ObjectReader) *kv2.ObjectResult {

	rs := kv2.NewObjectResultOK()

	t0 := time.Now()
	if cn.opts.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Read", time.Since(t0).Seconds())
		}()
	}

	if cn.monitor != nil {
		tp := timeus()
		defer func() {
			cn.monitor.Metric(MetricStorageLatency).With(map[string]string{
				"Read": "Key",
			}).Add(timeus() - tp)
		}()
	}

	tdb := cn.tabledb(rr.TableName)
	if tdb == nil {
		rs.StatusMessage(kv2.ResultClientError, "table not found")
		return rs
	}

	rs.LogVersion, _ = tdb.objectLogVersionSet(0, 0, 0)

	tn := timems()

	if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeKey) {

		for _, k := range rr.Keys {

			var (
				rs2 kv2.StorageResult
			)

			if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeMetaOnly) ||
				kv2.AttrAllow(rr.Attrs, kv2.ObjectMetaAttrDataOff) {
				rs2 = tdb.db.Get(keyEncode(nsKeyMeta, k), nil)
			} else {
				rs2 = tdb.db.Get(keyEncode(nsKeyData, k), nil)
			}

			if cn.monitor != nil {
				cn.monitor.Metric(MetricStorageCall).With(map[string]string{
					"Read": "Key",
				}).Add(int64(rs2.Len()))
			}

			if cn.opts.Server.MetricsEnable {
				metricCounter.Add(metricStorage, "Key.Read", 1)
				metricCounter.Add(metricStorageSize, "Key.Read", float64(rs2.Len()))
			}

			if rs2.OK() {

				item, err := kv2.ObjectItemDecode(rs2.Bytes())
				if err == nil {
					if item.Meta.Expired > 0 && item.Meta.Expired < uint64(tn) {
						if len(rr.Keys) == 1 {
							rs.StatusMessage(kv2.ResultNotFound, "")
						}
					} else {
						rs.Items = append(rs.Items, item)
					}
				} else {
					rs.StatusMessage(kv2.ResultServerError, err.Error())
				}

			} else {

				if !rs2.NotFound() {
					rs.StatusMessage(kv2.ResultServerError, rs2.ErrorMessage())
					break
				}

				if len(rr.Keys) == 1 {
					rs.StatusMessage(kv2.ResultNotFound, "")
				}
			}
		}

	} else if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeKeyRange) {

		if err := cn.objectQueryKeyRange(rr, rs); err != nil {
			rs.StatusMessage(kv2.ResultServerError, err.Error())
		}

	} else if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeLogRange) {

		if err := cn.objectQueryLogRange(rr, rs); err != nil {
			rs.StatusMessage(kv2.ResultServerError, err.Error())
		}

	} else {

		rs.StatusMessage(kv2.ResultClientError, "invalid mode")
	}

	if rs.Status == 0 {
		rs.Status = kv2.ResultOK
	}

	return rs
}

func (cn *Conn) objectQueryRemote(rr *kv2.ObjectReader) *kv2.ObjectResult {

	mainNodes := cn.opts.Cluster.randMainNodes(3)
	if len(mainNodes) < 1 {
		return kv2.NewObjectResultClientError(errors.New("no master found"))
	}

	for _, v := range mainNodes {

		conn, err := clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, false)
		if err != nil {
			continue
		}

		ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
		defer fc()

		rs, err := kv2.NewPublicClient(conn).Query(ctx, rr)
		if err != nil {
			return kv2.NewObjectResultServerError(err)
		}

		return rs
	}

	return kv2.NewObjectResultServerError(errors.New("no cluster nodes"))
}

func (cn *Conn) objectQueryKeyRange(rr *kv2.ObjectReader, rs *kv2.ObjectResult) error {

	t0 := time.Now()
	if cn.opts.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Key.Range", time.Since(t0).Seconds())
		}()
	}

	if cn.monitor != nil {
		tp := timeus()
		defer func() {
			cn.monitor.Metric(MetricStorageLatency).With(map[string]string{
				"Read": "KeyRange",
			}).Add(timeus() - tp)
		}()
	}

	tdb := cn.tabledb(rr.TableName)
	if tdb == nil {
		return errors.New("table not found")
	}

	nsKey := nsKeyData
	if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeMetaOnly) ||
		kv2.AttrAllow(rr.Attrs, kv2.ObjectMetaAttrDataOff) {
		nsKey = nsKeyMeta
	}

	var (
		offset    = keyEncode(nsKey, bytesClone(rr.KeyOffset))
		cutset    = keyEncode(nsKey, bytesClone(rr.KeyCutset))
		limitNum  = int(rr.LimitNum)
		limitSize = int(rr.LimitSize)
		readSize  int
	)

	if limitNum > int(kv2.ObjectReaderLimitNumMax) {
		limitNum = int(kv2.ObjectReaderLimitNumMax)
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = int(kv2.ObjectReaderLimitSizeDef)
	} else if limitSize > int(kv2.ObjectReaderLimitSizeMax) {
		limitSize = int(kv2.ObjectReaderLimitSizeMax)
	}

	var (
		iter kv2.StorageIterator
		tn   = timems()
	)

	if kv2.AttrAllow(rr.Mode, kv2.ObjectReaderModeRevRange) {

		// offset = append(offset, 0xff)

		iter = tdb.db.NewIterator(&kv2.StorageIteratorRange{
			Start: cutset,
			Limit: offset,
		})

		for ok := iter.Last(); ok; ok = iter.Prev() {

			if bytes.Compare(iter.Key(), offset) >= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) < 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			item, err := kv2.ObjectItemDecode(iter.Value())
			if err != nil {
				continue
			}
			if item.Meta.Expired > 0 && item.Meta.Expired < uint64(tn) {
				continue
			}

			if len(rs.Items) > 0 && readSize+len(iter.Value()) > int(limitSize) {
				rs.Next = true
				break
			}
			readSize += len(iter.Value())

			rs.Items = append(rs.Items, item)

			if len(rs.Items) >= limitNum || readSize >= limitSize {
				rs.Next = true
				break
			}
		}

	} else {

		cutset = append(cutset, 0xff)

		iter = tdb.db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})

		for ok := iter.First(); ok; ok = iter.Next() {

			if bytes.Compare(iter.Key(), offset) <= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) >= 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			item, err := kv2.ObjectItemDecode(iter.Value())
			if err != nil {
				continue
			}
			if item.Meta.Expired > 0 && item.Meta.Expired < uint64(tn) {
				continue
			}

			if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
				rs.Next = true
				break
			}
			readSize += len(iter.Value())

			rs.Items = append(rs.Items, item)

			if len(rs.Items) >= limitNum || readSize >= limitSize {
				rs.Next = true
				break
			}
		}
	}

	iter.Release()

	if iter.Error() != nil {
		return iter.Error()
	}

	if len(rs.Items) == 0 {
		rs.StatusMessage(kv2.ResultNotFound, "")
	}

	if cn.opts.Server.MetricsEnable {
		metricCounter.Add(metricStorage, "Key.Range", 1)
		metricCounter.Add(metricStorage, "Key.Range.Item", float64(len(rs.Items)))
		metricCounter.Add(metricStorageSize, "Key.Range", float64(readSize))
	}

	if cn.monitor != nil {
		cn.monitor.Metric(MetricStorageCall).With(map[string]string{
			"Read": "KeyRange",
		}).Add(int64(len(rs.Items)))
	}

	return nil
}

func (cn *Conn) objectQueryLogRange(rr *kv2.ObjectReader, rs *kv2.ObjectResult) error {

	t0 := time.Now()
	if cn.opts.Server.MetricsEnable {
		defer func() {
			metricLatency.Add(metricStorage, "Log.Range", time.Since(t0).Seconds())
		}()
	}

	if cn.monitor != nil {
		tp := timeus()
		defer func() {
			cn.monitor.Metric(MetricStorageLatency).With(map[string]string{
				"Read": "LogRange",
			}).Add(timeus() - tp)
		}()
	}

	tdb := cn.tabledb(rr.TableName)
	if tdb == nil {
		return errors.New("table not found")
	}

	var (
		offset    = keyEncode(nsKeyLog, uint64ToBytes(rr.LogOffset))
		cutset    = keyEncode(nsKeyLog, []byte{0xff})
		limitNum  = int(rr.LimitNum)
		limitSize = int(rr.LimitSize)
		readSize  int
	)

	if limitNum > int(kv2.ObjectReaderLimitNumMax) {
		limitNum = int(kv2.ObjectReaderLimitNumMax)
	} else if limitNum < 1 {
		limitNum = 1
	}

	if limitSize < 1 {
		limitSize = int(kv2.ObjectReaderLimitSizeDef)
	} else if limitSize > int(kv2.ObjectReaderLimitSizeMax) {
		limitSize = int(kv2.ObjectReaderLimitSizeMax)
	}

	if rr.WaitTime < 0 {
		rr.WaitTime = 0
	} else if rr.WaitTime > workerLogRangeWaitTimeMax {
		rr.WaitTime = workerLogRangeWaitTimeMax
	}

	for ; rr.WaitTime >= 0; rr.WaitTime -= workerLogRangeWaitSleep {

		if tdb.logOffset <= rr.LogOffset {
			time.Sleep(time.Duration(workerLogRangeWaitSleep) * time.Millisecond)
			continue
		}

		var (
			tto  = tdb.objectLogDelay() // uint64(time.Now().UnixNano()/1e6) - 3000
			iter = tdb.db.NewIterator(&kv2.StorageIteratorRange{
				Start: offset,
				Limit: cutset,
			})
		)

		for ok := iter.First(); ok; ok = iter.Next() {

			if bytes.Compare(iter.Key(), offset) <= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) >= 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			if len(rs.Items) > 0 && readSize+len(iter.Value()) > limitSize {
				rs.Next = true
				break
			}
			readSize += len(iter.Value())

			meta, _, err := kv2.ObjectMetaDecode(iter.Value())
			if err != nil || meta == nil {
				if err != nil {
					hlog.Printf("info", "db-log-range err %s", err.Error())
				}
				break
			}

			//
			if kv2.AttrAllow(meta.Attrs, kv2.ObjectMetaAttrDelete) {
				rs.Items = append(rs.Items, &kv2.ObjectItem{
					Meta: meta,
				})
			} else {

				var nsKey = nsKeyData
				if kv2.AttrAllow(meta.Attrs, kv2.ObjectMetaAttrDataOff) {
					nsKey = nsKeyMeta
				}

				ss := tdb.db.Get(keyEncode(nsKey, meta.Key), nil)

				/**
				if ss.NotFound() {
					if nsKey == nsKeyData {
						nsKey = nsKeyMeta
					} else {
						nsKey = nsKeyData
					}
					ss = tdb.db.Get(keyEncode(nsKey, meta.Key), nil)
				}
				*/

				if !ss.OK() {
					if !ss.NotFound() {
						hlog.Printf("errro", "db-log-range err %s", ss.ErrorMessage())
						break
					}
					continue
				}

				item, err := kv2.ObjectItemDecode(ss.Bytes())
				if err != nil {
					continue
				}

				if item.Meta.Version != meta.Version {
					continue
				}

				if item.Meta.Updated >= tto {
					break
				}

				if len(rs.Items) > 0 && readSize+ss.Len() > limitSize {
					rs.Next = true
					break
				}
				readSize += ss.Len()

				rs.Items = append(rs.Items, item)
			}

			if len(rs.Items) >= limitNum || readSize >= limitSize {
				rs.Next = true
				break
			}
		}

		iter.Release()

		if iter.Error() != nil {
			return iter.Error()
		}

		if len(rs.Items) > 0 || rr.WaitTime < 0 {
			break
		}

		if rr.WaitTime >= workerLogRangeWaitSleep {
			time.Sleep(time.Duration(workerLogRangeWaitSleep) * time.Millisecond)
		}
	}

	if cn.opts.Server.MetricsEnable {
		metricCounter.Add(metricStorage, "Log.Range", 1)
		metricCounter.Add(metricStorage, "Log.Range.Item", float64(len(rs.Items)))
		metricCounter.Add(metricStorageSize, "Log.Range", float64(readSize))
	}

	if cn.monitor != nil {
		cn.monitor.Metric(MetricStorageCall).With(map[string]string{
			"Read": "LogRange",
		}).Add(int64(readSize))
	}

	return nil
}

func (cn *Conn) NewReader(keys ...[]byte) *kv2.ClientReader {
	return kv2.NewClientReader(cn, keys...)
}

func (cn *Conn) NewWriter(key []byte, value interface{}, opts ...interface{}) *kv2.ClientWriter {
	return kv2.NewClientWriter(cn, key, value, opts...)
}

func (cn *Conn) objectMetaGet(rr *kv2.ObjectWriter) (*kv2.ObjectMeta, error) {

	tdb := cn.tabledb(rr.TableName)
	if tdb == nil {
		return nil, errors.New("table not found")
	}

	var nsKey = nsKeyMeta
	if kv2.AttrAllow(rr.Meta.Attrs, kv2.ObjectMetaAttrMetaOff) ||
		cn.opts.Feature.WriteMetaDisable {
		nsKey = nsKeyData
	}

	ss := tdb.db.Get(keyEncode(nsKey, rr.Meta.Key), nil)

	if ss.NotFound() {
		if nsKey == nsKeyData {
			nsKey = nsKeyMeta
		} else {
			nsKey = nsKeyData
		}
		ss = tdb.db.Get(keyEncode(nsKey, rr.Meta.Key), nil)
	}

	if ss.OK() {
		meta, _, err := kv2.ObjectMetaDecode(ss.Bytes())
		return meta, err
	}

	if ss.NotFound() {
		return nil, nil
	}

	return nil, ss.Error()
}

func (it *Conn) Connector() kv2.ClientConnector {
	return it
}
