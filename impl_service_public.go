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
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hooto/hauth/go/hauth/v1"
	"google.golang.org/grpc"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

type PublicServiceImpl struct {
	kv2.UnimplementedPublicServer
	server     *grpc.Server
	db         *Conn
	prepares   map[string]*kv2.ObjectWriter
	proposalMu sync.RWMutex
	sock       net.Listener
}

type pQueItem struct {
	Log uint64
	Inc uint64
}

func (it *PublicServiceImpl) Query(ctx context.Context,
	or *kv2.ObjectReader) (*kv2.ObjectResult, error) {

	tp := timeus()

	if ctx != nil {

		av, err := appAuthParse(ctx, it.db.keyMgr)
		if err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if err := av.SignValid(nil); err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if or.TableName == "sys" && av.Allow(authPermSysAll) != nil {
			return kv2.NewObjectResultAccessDenied(), nil
		}

		if err := av.Allow(authPermTableRead,
			hauth.NewScopeFilter(AuthScopeTable, or.TableName)); err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}
	}

	rs := it.db.Query(or)

	if it.db.monitor != nil {

		labels := map[string]string{}
		siz := resultDataSize(rs)

		if kv2.AttrAllow(or.Mode, kv2.ObjectReaderModeKey) {
			labels["Read"] = "Key"
		} else if kv2.AttrAllow(or.Mode, kv2.ObjectReaderModeKeyRange) {
			labels["Read"] = "KeyRange"
		} else if kv2.AttrAllow(or.Mode, kv2.ObjectReaderModeLogRange) {
			labels["Read"] = "LogRange"
		}

		if len(labels) > 0 {
			it.db.monitor.Metric(MetricServiceCall).With(labels).Add(siz)
			it.db.monitor.Metric(MetricServiceLatency).With(labels).Add(timeus() - tp)
		}
	}

	return rs, nil
}

func (it *PublicServiceImpl) Commit(ctx context.Context,
	rr *kv2.ObjectWriter) (*kv2.ObjectResult, error) {

	metricLabels := map[string]string{"Write": "Key"}
	if it.db.monitor != nil {
		tp := timeus()
		defer func() {
			it.db.monitor.Metric(MetricServiceLatency).With(metricLabels).Add(timeus() - tp)
		}()
	}

	if ctx != nil {

		av, err := appAuthParse(ctx, it.db.keyMgr)
		if err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if err := av.SignValid(nil); err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if rr.TableName == "sys" && av.Allow(authPermSysAll) != nil {
			return kv2.NewObjectResultAccessDenied(), nil
		}

		if err := av.Allow(authPermTableWrite,
			hauth.NewScopeFilter(AuthScopeTable, rr.TableName)); err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}
	}

	var rs *kv2.ObjectResult

	if len(it.db.opts.Cluster.MainNodes) == 0 {
		rs = it.db.Commit(rr)
	} else {
		rs = it.groupCommit(ctx, rr)
	}

	if it.db.monitor != nil {

		siz := int64(0)
		if rr.Data != nil && len(rr.Data.Value) > 0 {
			siz = int64(len(rr.Data.Value))
		}

		it.db.monitor.Metric(MetricServiceCall).With(metricLabels).Add(siz)
	}

	return rs, nil
}

func (it *PublicServiceImpl) groupCommit(ctx context.Context,
	rr *kv2.ObjectWriter) *kv2.ObjectResult {

	if err := rr.CommitValid(); err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	meta, err := it.db.objectMetaGet(rr)
	if meta == nil && err != nil {
		return kv2.NewObjectResultServerError(err)
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

		if kv2.AttrAllow(rr.Mode, kv2.ObjectWriterModeCreate) ||
			(rr.Meta.Updated < meta.Updated) ||
			(rr.Meta.Expired == meta.Expired &&
				(rr.Meta.IncrId == 0 || rr.Meta.IncrId == meta.IncrId) &&
				(rr.PrevIncrId == 0 || rr.PrevIncrId == meta.IncrId) &&
				meta.DataCheck == rr.Meta.DataCheck) {

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
	}

	if rr.Meta.Created == 0 {
		rr.Meta.Created = rr.Meta.Updated
	}

	var (
		nCap = len(it.db.opts.Cluster.MainNodes)
		pNum = 0
		pLog = uint64(0)
		pInc = uint64(0)
		pQue = make(chan pQueItem, nCap+1)
		pTTL = time.Millisecond * time.Duration(objAcceptTTL)
	)

	for _, v := range it.db.opts.Cluster.MainNodes {

		go func(v *ClientConfig, rr *kv2.ObjectWriter) {

			conn, err := clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, false)
			var rs *kv2.ObjectResult

			if err == nil {
				ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
				defer fc()
				rs, err = kv2.NewInternalClient(conn).Prepare(ctx, rr)
				if err != nil {
					conn, err = clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, true)
					rs, err = kv2.NewInternalClient(conn).Prepare(ctx, rr)
				}
			}

			if err == nil && rs.Meta != nil && rs.Meta.Version > 0 {
				pQue <- pQueItem{
					Log: rs.Meta.Version,
					Inc: rs.Meta.IncrId,
				}
			} else {
				pQue <- pQueItem{
					Log: 0,
				}
			}
		}(v, rr)
	}

	for {

		select {
		case v := <-pQue:
			if v.Log > 0 {
				pNum += 1
				if v.Log > pLog {
					pLog = v.Log
				}
				if v.Inc > pInc {
					pInc = v.Inc
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
		return kv2.NewObjectResultServerError(fmt.Errorf("p1 fail %d/%d", pNum, nCap))
	}

	pNum = 0
	pTTL = time.Millisecond * time.Duration(objAcceptTTL)
	pQue2 := make(chan uint64, nCap+1)

	rr2 := kv2.NewObjectWriter(rr.Meta.Key, nil)
	rr2.Meta.Version = pLog
	rr2.Meta.IncrId = pInc

	for _, v := range it.db.opts.Cluster.MainNodes {

		go func(v *ClientConfig, rr *kv2.ObjectWriter) {

			conn, err := clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, false)
			var rs *kv2.ObjectResult
			if err == nil {
				ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
				defer fc()
				rs, err = kv2.NewInternalClient(conn).Accept(ctx, rr2)
				if err != nil {
					conn, err = clientConn(v.Addr, v.AccessKey, v.AuthTLSCert, true)
					rs, err = kv2.NewInternalClient(conn).Accept(ctx, rr2)
				}
			}

			if err == nil && rs.Meta != nil && rs.Meta.Version == pLog {
				pQue2 <- 1
			} else {
				pQue2 <- 0
			}
		}(v, rr)
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
		return kv2.NewObjectResultServerError(fmt.Errorf("p2 fail %d/%d", pNum, nCap))
	}

	rs := kv2.NewObjectResultOK()
	rs.Meta = &kv2.ObjectMeta{
		Version: pLog,
		IncrId:  pInc,
		Updated: rr.Meta.Updated,
	}

	return rs
}

func (it *PublicServiceImpl) BatchCommit(ctx context.Context,
	rr *kv2.BatchRequest) (*kv2.BatchResult, error) {

	tp := timeus()

	if ctx != nil {

		av, err := appAuthParse(ctx, it.db.keyMgr)
		if err != nil {
			return kv2.NewBatchResultAccessDenied(err.Error()), nil
		}

		if err := av.SignValid(nil); err != nil {
			return kv2.NewBatchResultAccessDenied(err.Error()), nil
		}

		for _, v := range rr.Items {

			if v.Reader != nil {

				if v.Reader.TableName == "sys" && av.Allow(authPermSysAll) != nil {
					return kv2.NewBatchResultAccessDenied(), nil
				}

				if err := av.Allow(authPermTableRead,
					hauth.NewScopeFilter(AuthScopeTable, v.Reader.TableName)); err != nil {
					return kv2.NewBatchResultAccessDenied(err.Error()), nil
				}

			} else if v.Writer != nil {

				if v.Writer.TableName == "sys" && av.Allow(authPermSysAll) != nil {
					return kv2.NewBatchResultAccessDenied(), nil
				}

				if err := av.Allow(authPermTableWrite,
					hauth.NewScopeFilter(AuthScopeTable, v.Writer.TableName)); err != nil {
					return kv2.NewBatchResultAccessDenied(), nil
				}
			}
		}
	}

	var rs *kv2.BatchResult

	if len(rr.Items) == 0 {
		rs = rr.NewResult(kv2.ResultOK, "")
	} else if len(it.db.opts.Cluster.MainNodes) == 0 {
		rs = it.db.BatchCommit(rr)
	} else {
		rs = it.groupBatchCommit(ctx, rr)
	}

	if it.db.monitor != nil {

		bsiz := batchResultDataSize(rs)
		it.db.monitor.Metric(MetricServiceCall).With(map[string]string{
			"Read": "BatchKey",
		}).Add(bsiz)

		it.db.monitor.Metric(MetricServiceLatency).With(map[string]string{
			"Read": "BatchKey",
		}).Add(timeus() - tp)

		wsiz := 0
		for _, b := range rr.Items {
			if b.Writer != nil && b.Writer.Data != nil {
				wsiz += len(b.Writer.Data.Value)
			}
		}

		it.db.monitor.Metric(MetricServiceCall).With(map[string]string{
			"Write": "BatchKey",
		}).Add(int64(wsiz))

		it.db.monitor.Metric(MetricServiceLatency).With(map[string]string{
			"Write": "BatchKey",
		}).Add(timeus() - tp)
	}

	return rs, nil
}

func (it *PublicServiceImpl) groupBatchCommit(ctx context.Context,
	rr *kv2.BatchRequest) *kv2.BatchResult {

	var (
		rs = rr.NewResult(0, "")
		ok = 0
	)

	for _, v := range rr.Items {

		var (
			rs2 *kv2.ObjectResult
			err error
		)

		if v.Reader != nil {

			if v.Reader.TableName == "" {
				v.Reader.TableName = rr.TableName
			}
			rs2, err = it.Query(nil, v.Reader)

		} else if v.Writer != nil {

			if v.Writer.TableName == "" {
				v.Writer.TableName = rr.TableName
			}
			rs2, err = it.Commit(nil, v.Writer)

		} else {
			rs2 = kv2.NewObjectResultClientError(errors.New("no reader/writer commit"))
		}

		rs.Items = append(rs.Items, rs2)

		if err == nil && rs2.OK() {
			ok += 1
		} else if err != nil || rs2.Message != "" {
			if rs.Message != "" {
				rs.Message += ", "
			}
			if err != nil {
				rs.Message += err.Error()
			} else {
				rs.Message += rs2.Message
			}
		}
	}

	if ok == len(rs.Items) {
		rs.Status = kv2.ResultOK
	}

	return rs
}

func (it *PublicServiceImpl) SysCmd(ctx context.Context, req *kv2.SysCmdRequest) (*kv2.ObjectResult, error) {

	var (
		av  *hauth.AppValidator
		err error
	)

	if ctx != nil {

		av, err = appAuthParse(ctx, it.db.keyMgr)
		if err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if err := av.SignValid(nil); err != nil {
			return kv2.NewObjectResultAccessDenied(err.Error()), nil
		}

		if !strings.HasPrefix(req.Method, "Table") &&
			av.Allow(authPermSysAll) != nil {
			return kv2.NewObjectResultAccessDenied(), nil
		}
	}

	if len(it.db.opts.Cluster.MainNodes) == 0 {
		return it.db.sysCmdLocal(av, req), nil
	}

	rs := kv2.NewObjectResultOK()

	return rs, nil
}
