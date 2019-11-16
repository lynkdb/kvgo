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
	"sync"
	"time"

	"github.com/lynkdb/iomix/sko"
	"github.com/syndtr/goleveldb/leveldb"
	grpc "google.golang.org/grpc"
)

type SkoServiceImpl struct {
	server     *grpc.Server
	db         *SkoConn
	prepares   map[string]*sko.ObjectWriter
	proposalMu sync.RWMutex
	sock       net.Listener
}

var (
	skoGrpcMsgByteMax  = 12 * 1024 * 1024
	skoGrpcClientConns = map[string]*grpc.ClientConn{}
	skoClientConnMu    sync.Mutex
)

func (cn *SkoConn) ClusterStart() error {

	if cn.opts.ClusterBind == "" {
		return nil
	}

	_, port, err := net.SplitHostPort(cn.opts.ClusterBind)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	server := grpc.NewServer(
		grpc.MaxMsgSize(skoGrpcMsgByteMax),
		grpc.MaxSendMsgSize(skoGrpcMsgByteMax),
		grpc.MaxRecvMsgSize(skoGrpcMsgByteMax),
	)

	go server.Serve(lis)

	cn.skoCluster = &SkoServiceImpl{
		sock:     lis,
		server:   server,
		db:       cn,
		prepares: map[string]*sko.ObjectWriter{},
	}

	sko.RegisterObjectServer(server, cn.skoCluster)

	go cn.skoClusterWorker()

	return nil
}

func ClientConn(addr string) (*grpc.ClientConn, error) {

	skoClientConnMu.Lock()
	defer skoClientConnMu.Unlock()

	if c, ok := skoGrpcClientConns[addr]; ok {
		return c, nil
	}

	c, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithMaxMsgSize(skoGrpcMsgByteMax),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(skoGrpcMsgByteMax)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(skoGrpcMsgByteMax)),
	)
	if err != nil {
		return nil, err
	}

	skoGrpcClientConns[addr] = c

	return c, nil
}

func (it *SkoServiceImpl) Query(ctx context.Context,
	or *sko.ObjectReader) (*sko.ObjectResult, error) {
	return it.db.Query(or), nil
}

type pQueItem struct {
	Log uint64
	Inc uint64
}

func (it *SkoServiceImpl) Commit(ctx context.Context,
	rr *sko.ObjectWriter) (*sko.ObjectResult, error) {

	if err := rr.CommitValid(); err != nil {
		return sko.NewObjectResultClientError(err), nil
	}

	meta, err := it.db.objectMetaGet(rr)
	if meta == nil && err != nil {
		return sko.NewObjectResultServerError(err), nil
	}

	if meta == nil {

		if sko.AttrAllow(rr.Mode, sko.ObjectWriterModeDelete) {
			return sko.NewObjectResultOK(), nil
		}

	} else {

		if sko.AttrAllow(rr.Mode, sko.ObjectWriterModeCreate) {
			rs := sko.NewObjectResultOK()
			rs.Meta = &sko.ObjectMeta{
				Version: meta.Version,
				IncrId:  meta.IncrId,
				Created: meta.Created,
			}
			return rs, nil
		}

		if meta.DataCheck == rr.Meta.DataCheck {
			rs := sko.NewObjectResultOK()
			rs.Meta = &sko.ObjectMeta{
				Version: meta.Version,
				IncrId:  meta.IncrId,
			}
			return rs, nil
		}

		if meta.Created > 0 {
			rr.Meta.Created = meta.Created
		}
	}

	if rr.Meta.Created == 0 {
		rr.Meta.Created = rr.Meta.Updated
	}

	var (
		nCap = len(it.db.opts.ClusterNodes)
		pNum = 0
		pLog = uint64(0)
		pInc = uint64(0)
		pQue = make(chan pQueItem, nCap+1)
		pTTL = time.Second * 3
	)

	for _, addr := range it.db.opts.ClusterNodes {

		conn, err := ClientConn(addr)
		if err != nil {
			continue
		}

		go func(conn *grpc.ClientConn, rr *sko.ObjectWriter) {
			ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
			defer fc()

			rs, err := sko.NewObjectClient(conn).Prepare(ctx, rr)
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
		}(conn, rr)
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
			break
		}
	}

	if (pNum * 2) <= nCap {
		return nil, fmt.Errorf("p1 fail %d/%d", pNum, nCap)
	}

	pNum = 0
	pTTL = time.Second * 3
	pQue2 := make(chan uint64, nCap+1)

	rr2 := sko.NewObjectWriter(rr.Meta.Key, nil)
	rr2.Meta.Version = pLog
	rr2.Meta.IncrId = pInc

	for _, addr := range it.db.opts.ClusterNodes {

		conn, err := ClientConn(addr)
		if err != nil {
			continue
		}

		go func(conn *grpc.ClientConn, rr *sko.ObjectWriter) {
			ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
			defer fc()

			rs, err := sko.NewObjectClient(conn).Accept(ctx, rr2)
			if err == nil && rs.Meta != nil && rs.Meta.Version == pLog {
				pQue2 <- 1
			} else {
				pQue2 <- 0
			}
		}(conn, rr)
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
			break
		}
	}

	if (pNum * 2) <= nCap {
		return nil, fmt.Errorf("p2 fail %d/%d", pNum, nCap)
	}

	rs := sko.NewObjectResultOK()
	rs.Meta = &sko.ObjectMeta{
		Version: pLog,
		IncrId:  pInc,
	}

	return rs, nil
}

func (it *SkoServiceImpl) Prepare(ctx context.Context,
	or *sko.ObjectWriter) (*sko.ObjectResult, error) {

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	tn := uint64(time.Now().UnixNano() / 1e6)

	if len(it.prepares) > 10 {
		dels := []string{}
		for k, v := range it.prepares {
			if (v.ProposalExpired + 3000) < tn {
				dels = append(dels, k)
			}
		}
		for _, k := range dels {
			delete(it.prepares, k)
		}
	}

	p, ok := it.prepares[string(or.Meta.Key)]
	if ok && (p.ProposalExpired+3000) > tn {
		return nil, errors.New("deny")
	}

	pLog, err := it.db.objectLogVersionSet(1, 0)
	if err != nil {
		return nil, err
	}

	pInc := or.Meta.IncrId
	if or.IncrNamespace != "" && or.Meta.IncrId == 0 {
		pInc, err = it.db.objectIncrSet(or.IncrNamespace, 1, 0)
		if err != nil {
			return nil, err
		}
	}

	or.ProposalExpired = tn + 3000

	it.prepares[string(or.Meta.Key)] = or

	rs := sko.NewObjectResultOK()
	rs.Meta = &sko.ObjectMeta{
		Version: pLog,
		IncrId:  pInc,
	}
	return rs, nil
}

func (it *SkoServiceImpl) Accept(ctx context.Context,
	rr2 *sko.ObjectWriter) (*sko.ObjectResult, error) {

	it.proposalMu.Lock()
	defer it.proposalMu.Unlock()

	if rr2.Meta == nil {
		return nil, errors.New("invalid request")
	}

	var (
		tn   = uint64(time.Now().UnixNano() / 1e6)
		cLog = rr2.Meta.Version
		cInc = rr2.Meta.IncrId
	)

	rr, ok := it.prepares[string(rr2.Meta.Key)]
	if !ok || (rr.ProposalExpired+3000) < tn {
		return nil, errors.New("deny")
	}

	if rr.Meta.Version > cLog {
		return nil, errors.New("invalid version")
	}

	it.db.objectLogVersionSet(0, cLog)
	if rr.IncrNamespace != "" && cInc > 0 {
		it.db.objectIncrSet(rr.IncrNamespace, 0, cInc)
	}

	it.db.skoMu.Lock()
	defer it.db.skoMu.Unlock()

	meta, err := it.db.objectMetaGet(rr)
	if meta == nil && err != nil {
		return nil, err
	}
	if meta != nil && meta.Version > cLog {
		return nil, errors.New("invalid version")
	}

	rr.Meta.Version = cLog
	rr.Meta.IncrId = cInc

	if sko.AttrAllow(rr.Mode, sko.ObjectWriterModeDelete) {

		rr.Meta.Attrs = sko.ObjectMetaAttrDelete

		if bsMeta, err := rr.MetaEncode(); err == nil {

			batch := new(leveldb.Batch)

			if meta != nil {
				batch.Delete(t_ns_cat(ns_sko_meta, rr.Meta.Key))
				batch.Delete(t_ns_cat(ns_sko_data, rr.Meta.Key))
				batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
			}

			batch.Put(t_ns_cat(ns_sko_log, uint64_to_bytes(cLog)), bsMeta)

			err = it.db.db.Write(batch, nil)
		}

	} else {

		if bsMeta, bsData, err := rr.PutEncode(); err == nil {

			batch := new(leveldb.Batch)

			batch.Put(t_ns_cat(ns_sko_meta, rr.Meta.Key), bsMeta)
			batch.Put(t_ns_cat(ns_sko_data, rr.Meta.Key), bsData)
			batch.Put(t_ns_cat(ns_sko_log, uint64_to_bytes(cLog)), bsMeta)

			if rr.Meta.Expired > 0 {
				batch.Put(keyExpireEncode(ns_sko_ttl, rr.Meta.Expired, rr.Meta.Key), bsMeta)
			}

			if meta != nil {
				if meta.Version < cLog {
					batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
				}
				if meta.Expired > 0 && meta.Expired != rr.Meta.Expired {
					batch.Delete(keyExpireEncode(ns_sko_ttl, meta.Expired, rr.Meta.Key))
				}
			}

			err = it.db.db.Write(batch, nil)
		}
	}

	if err != nil {
		return nil, err
	}

	delete(it.prepares, string(rr2.Meta.Key))

	rs := sko.NewObjectResultOK()
	rs.Meta = &sko.ObjectMeta{
		Version: cLog,
		IncrId:  cInc,
	}

	return rs, nil
}
