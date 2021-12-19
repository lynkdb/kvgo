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
	"net"
	"sync"
	"time"

	"github.com/hooto/hlog4g/hlog"
	"google.golang.org/grpc"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

type InternalServiceImpl struct {
	kv2.UnimplementedInternalServer
	server     *grpc.Server
	db         *Conn
	prepares   map[string]*kv2.ObjectWriter
	proposalMu sync.RWMutex
	sock       net.Listener
}

func (it *InternalServiceImpl) Prepare(ctx context.Context,
	or *kv2.ObjectWriter) (*kv2.ObjectResult, error) {

	if err := appAuthValid(ctx, it.db.keyMgr); err != nil {
		return kv2.NewObjectResultClientError(err), nil
	}

	tdb := it.db.tabledb(or.TableName)
	if tdb == nil {
		return kv2.NewObjectResultClientError(errors.New("table not found")), nil
	}

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	tn := uint64(time.Now().UnixNano() / 1e6)

	if len(it.prepares) > 10 {
		dels := []string{}
		for k, v := range it.prepares {
			if (v.ProposalExpired + objAcceptTTL) < tn {
				dels = append(dels, k)
			}
		}
		for _, k := range dels {
			delete(it.prepares, k)
		}
	}

	p, ok := it.prepares[string(or.Meta.Key)]
	if ok && (p.ProposalExpired+objAcceptTTL) > tn {
		return nil, errors.New("deny")
	}

	pLog, err := tdb.objectLogVersionSet(1, 0, tn)
	if err != nil {
		return nil, err
	}

	pInc := or.Meta.IncrId
	if or.IncrNamespace != "" && or.Meta.IncrId == 0 {
		pInc, err = tdb.objectIncrSet(or.IncrNamespace, 1, 0)
		if err != nil {
			return nil, err
		}
	}

	tdb.logSyncBuffer.put(pLog, or.Meta.Attrs, or.Meta.Key, false)

	or.ProposalExpired = tn + objAcceptTTL

	it.prepares[string(or.Meta.Key)] = or

	rs := kv2.NewObjectResultOK()
	rs.Meta = &kv2.ObjectMeta{
		Version: pLog,
		IncrId:  pInc,
	}
	return rs, nil
}

func (it *InternalServiceImpl) Accept(ctx context.Context,
	rr2 *kv2.ObjectWriter) (*kv2.ObjectResult, error) {

	if err := appAuthValid(ctx, it.db.keyMgr); err != nil {
		return kv2.NewObjectResultClientError(err), nil
	}

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	if rr2.Meta == nil {
		return nil, errors.New("invalid request")
	}

	var (
		tn     = uint64(time.Now().UnixNano() / 1e6)
		cLog   = rr2.Meta.Version
		cLogOn = true
		cInc   = rr2.Meta.IncrId
	)

	rr, ok := it.prepares[string(rr2.Meta.Key)]
	if !ok || (rr.ProposalExpired+objAcceptTTL) < tn {
		return nil, errors.New("deny")
	}

	if rr.Meta.Version > cLog {
		return nil, errors.New("invalid version")
	}

	tdb := it.db.tabledb(rr.TableName)
	if tdb == nil {
		return nil, errors.New("table not found")
	}

	tdb.objectLogVersionSet(0, cLog, tn)
	if rr.IncrNamespace != "" && cInc > 0 {
		tdb.objectIncrSet(rr.IncrNamespace, 0, cInc)
	}

	it.db.mu.Lock()
	defer it.db.mu.Unlock()

	meta, err := it.db.objectMetaGet(rr)
	if meta == nil && err != nil {
		return nil, err
	}
	if meta != nil && meta.Version > cLog {
		return nil, errors.New("invalid version")
	}

	if rr.Meta.Updated < 1 {
		rr.Meta.Updated = tn
	}

	if rr.Meta.Created < 1 {
		rr.Meta.Created = tn
	}

	rr.Meta.Version = cLog
	rr.Meta.IncrId = cInc

	if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeLogOff) {
		cLogOn = false
	}

	if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeDelete) {

		rr.Meta.Attrs |= kv2.ObjectMetaAttrDelete

		if bsMeta, err := rr.MetaEncode(); err == nil {

			batch := tdb.db.NewBatch()

			if meta != nil {
				batch.Delete(keyEncode(nsKeyMeta, rr.Meta.Key))
				batch.Delete(keyEncode(nsKeyData, rr.Meta.Key))
				batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
			}

			if cLogOn {
				batch.Put(keyEncode(nsKeyLog, uint64ToBytes(cLog)), bsMeta)
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

			if cLogOn && !it.db.opts.Feature.WriteLogDisable {
				batch.Put(keyEncode(nsKeyLog, uint64ToBytes(cLog)), bsMeta)
			}

			if rr.Meta.Expired > 0 {
				batch.Put(keyExpireEncode(nsKeyTtl, rr.Meta.Expired, rr.Meta.Key), bsMeta)
			}

			if meta != nil {
				if meta.Version < cLog && !it.db.opts.Feature.WriteLogDisable {
					batch.Delete(keyEncode(nsKeyLog, uint64ToBytes(meta.Version)))
				}
				if meta.Expired > 0 && meta.Expired != rr.Meta.Expired {
					batch.Delete(keyExpireEncode(nsKeyTtl, meta.Expired, rr.Meta.Key))
				}
			}

			if err = batch.Commit(); err == nil {
				tdb.objectLogFree(cLog)
			}
			if rr.Meta.Expired > 0 {
				tdb.expiredSync(int64(rr.Meta.Expired))
			}
		}
	}

	if err != nil {
		return nil, err
	}

	delete(it.prepares, string(rr2.Meta.Key))

	tdb.logSyncBuffer.hit(cLog)

	rs := kv2.NewObjectResultOK()
	rs.Meta = &kv2.ObjectMeta{
		Version: cLog,
		IncrId:  cInc,
	}

	return rs, nil
}

func (it *InternalServiceImpl) LogSync(ctx context.Context,
	req *kv2.LogSyncRequest) (*kv2.LogSyncReply, error) {

	if err := appAuthValid(ctx, it.db.keyMgr); err != nil {
		return nil, err
	}

	if !hex16.MatchString(req.ServerId) {
		return nil, errors.New("server id not found")
	}

	tdb := it.db.tabledb(req.TableName)
	if tdb == nil {
		return nil, errors.New("table not found")
	}

	if tdb.logSyncBuffer == nil {
		return nil, errors.New("logSyncBuffer not setup")
	}

	if len(req.KeyOffset) > 0 {

		if it.db.close {
			return nil, errors.New("db closed")
		}

		nsKey := uint8(0)
		if kv2.AttrAllow(req.Attrs, kv2.ObjectMetaAttrDataOff) {
			nsKey = nsKeyMeta
		} else if kv2.AttrAllow(req.Attrs, kv2.ObjectMetaAttrMetaOff) {
			nsKey = nsKeyData
		} else {
			return nil, errors.New("bad request (attrs)")
		}

		var (
			offset = keyEncode(nsKey, req.KeyOffset)
			cutset = keyEncode(nsKey, bytes.Repeat([]byte{0xff}, 32))
			num    = 1000
			siz    = 2 * 1024 * 1024
			dbsiz  = 0
			iter   = tdb.db.NewIterator(&kv2.StorageIteratorRange{
				Start: offset,
				Limit: cutset,
			})
			rs = &kv2.LogSyncReply{}
		)

		for ok := iter.First(); ok && num > 0 && siz > 0; ok = iter.Next() {
			num--

			if bytes.Compare(iter.Key(), offset) <= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) > 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			dbsiz += len(iter.Value())
			meta, err := kv2.ObjectMetaDecode(iter.Value())
			if err != nil || meta == nil {
				if err != nil {
					hlog.Printf("info", "db-log-range err %s", err.Error())
				}
				break
			}

			if meta.Updated == 0 {
				meta.Updated = uint64(timems())
			}

			rs.Logs = append(rs.Logs, &kv2.ObjectMeta{
				Version: meta.Version,
				Key:     bytesClone(meta.Key),
				Attrs:   meta.Attrs,
				Updated: meta.Updated,
			})
			siz -= (len(meta.Key) + 27)
		}

		rs.LogCutset, _ = tdb.objectLogVersionSet(0, 0, 0)

		iter.Release()

		if iter.Error() != nil {
			return nil, iter.Error()
		}

		if it.db.monitor != nil {
			it.db.monitor.Metric(MetricStorageCall).With(map[string]string{
				"Read": "KeyRange",
			}).Add(int64(dbsiz))
		}

		return rs, nil
	}

	if len(req.Keys) > 0 {

		var (
			siz = 4 * 1024 * 1024
			i   = 0
			rs  = &kv2.LogSyncReply{}
		)

		for ; i < len(req.Keys); i++ {

			k := req.Keys[i]

			nsKey := uint8(0)
			if kv2.AttrAllow(k.Attrs, kv2.ObjectMetaAttrDataOff) {
				nsKey = nsKeyMeta
			} else {
				nsKey = nsKeyData
			}

			ss := tdb.db.Get(keyEncode(nsKey, k.Key), nil)
			if ss.NotFound() && nsKey != nsKeyMeta {
				ss = tdb.db.Get(keyEncode(nsKeyMeta, k.Key), nil)
			}

			if !ss.OK() && !ss.NotFound() {
				return nil, ss.Error()
			}

			if ss.OK() {

				siz -= ss.Len()
				if siz < 0 {
					break
				}

				if it.db.monitor != nil {
					it.db.monitor.Metric(MetricStorageCall).With(map[string]string{
						"Read": "Key",
					}).Add(int64(ss.Len()))
				}

				if item, err := kv2.ObjectItemDecode(ss.Bytes()); err == nil {
					rs.Items = append(rs.Items, item)
				}
			}
		}

		if i < len(req.Keys) {
			rs.NextKeys = req.Keys[i+1:]
		}

		if p := tdb.logSyncBuffer.status(req.ServerId, 0, len(rs.Items)); p != nil && p.keyNum > 0 {
			hlog.SlotPrint(600, "info", "log sync reply to %s/%s cold keys %d, next keys %d",
				req.ServerId, req.TableName, p.keyNum, len(rs.NextKeys))
		}

		return rs, nil
	}

	// TODO rs := tdb.logSyncBuffer.query(req)
	rs := &kv2.LogSyncReply{
		//
	}
	if true {

		if it.db.close {
			return nil, errors.New("db closed")
		}

		var (
			offset = keyEncode(nsKeyLog, uint64ToBytes(req.LogOffset))
			cutset = append(offset, 0xff) // keyEncode(nsKeyLog, uint64ToBytes(rs.LogCutset))
			num    = 1000
			siz    = 2 * 1024 * 1024
			dbsiz  = 0
			iter   = tdb.db.NewIterator(&kv2.StorageIteratorRange{
				Start: offset,
				Limit: cutset,
			})
			delay = uint64(timems() - syncLogPullDelayMilSec)
		)

		for ok := iter.First(); ok && num > 0 && siz > 0; ok = iter.Next() {
			num--

			if bytes.Compare(iter.Key(), offset) <= 0 {
				continue
			}

			if bytes.Compare(iter.Key(), cutset) > 0 {
				break
			}

			if len(iter.Value()) < 2 {
				continue
			}

			dbsiz += len(iter.Value())
			meta, err := kv2.ObjectMetaDecode(iter.Value())
			if err != nil || meta == nil {
				if err != nil {
					hlog.Printf("info", "db-log-range err %s", err.Error())
				}
				break
			}

			if meta.Updated == 0 {
				meta.Updated = uint64(timems())
			} else if meta.Updated > delay {
				break
			}

			rs.Logs = append(rs.Logs, &kv2.ObjectMeta{
				Version: meta.Version,
				Key:     bytesClone(meta.Key),
				Attrs:   meta.Attrs,
				Updated: meta.Updated,
			})
			rs.LogCutset = meta.Version
			siz -= (len(meta.Key) + 27)
		}

		iter.Release()

		if iter.Error() != nil {
			return nil, iter.Error()
		}

		if it.db.monitor != nil {
			it.db.monitor.Metric(MetricStorageCall).With(map[string]string{
				"Read": "LogRange",
			}).Add(int64(dbsiz))
		}
	}

	if p := tdb.logSyncBuffer.status(req.ServerId, len(rs.Logs), 0); p != nil && p.logNum > 0 {
		hlog.SlotPrint(600, "info", "log sync reply to %s/%s cold logs %d, range %d ~ %d",
			req.ServerId, req.TableName, p.logNum, rs.LogOffset, rs.LogCutset)
	}

	return rs, nil
}
