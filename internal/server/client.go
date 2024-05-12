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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hooto/hauth/go/hauth/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

var (
	rpcClientConns = map[string]*grpc.ClientConn{}
	rpcClientMu    sync.Mutex

	clientMut sync.Mutex
	// map : address + key.id + [database] -> client
	clientConns = map[string]*internalClientConn{}
)

type internalClientConn struct {
	_ak      string
	cfg      *ConfigTransferSource
	rpcConn  *grpc.ClientConn
	database string
	kvClient kvapi.KvgoInternalClient
	err      error
}

func (it *ConfigTransferSource) newClient() (*internalClientConn, error) {

	if it.AccessKey == nil {
		return nil, errors.New("access key not setup")
	}

	ak := fmt.Sprintf("%s.%s", it.Addr, it.AccessKey.Id)

	clientMut.Lock()
	defer clientMut.Unlock()

	if clientConns == nil {
		clientConns = map[string]*internalClientConn{}
	}

	dbConn, ok := clientConns[ak]
	if !ok {

		conn, err := rpcClientConnect(it.Addr, it.AccessKey, false)
		if err != nil {
			return nil, err
		}

		if it.Options == nil {
			it.Options = kvapi.DefaultClientOptions()
			it.Options.Timeout = 20000
		}

		dbConn = &internalClientConn{
			_ak:      ak,
			cfg:      it,
			rpcConn:  conn,
			kvClient: kvapi.NewKvgoInternalClient(conn),
		}
		clientConns[ak] = dbConn
	}

	return dbConn, nil
}

func (it *ConfigTransferSource) timeout() time.Duration {
	if it.Options == nil {
		it.Options = kvapi.DefaultClientOptions()
		it.Options.Timeout = 20000
	}
	return time.Millisecond * time.Duration(it.Options.Timeout)
}

func (it *internalClientConn) innerLogRange(req *kvapi.LogRangeRequest) *kvapi.LogRangeResponse {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Database == "" {
		req.Database = it.database
	}

	rs, err := it.kvClient.LogRange(ctx, req, grpc.UseCompressor(gzip.Name))
	if err != nil {
		return newLogRangeResponse(kvapi.Status_ServerError, err.Error())
	}

	return rs
}

func (it *internalClientConn) innerRead(req *kvapi.InnerReadRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Database == "" {
		req.Database = it.database
	}

	rs, err := it.kvClient.Read(ctx, req, grpc.UseCompressor(gzip.Name))
	if err != nil {
		return newResultSet(kvapi.Status_ServerError, err.Error())
	}

	return rs
}

func (it *internalClientConn) innerRange(req *kvapi.RangeRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Database == "" {
		req.Database = it.database
	}

	rs, err := it.kvClient.Range(ctx, req, grpc.UseCompressor(gzip.Name))
	if err != nil {
		return newResultSet(kvapi.Status_ServerError, err.Error())
	}

	return rs
}

func rpcClientConnect(addr string,
	key *hauth.AccessKey,
	forceNew bool) (*grpc.ClientConn, error) {

	if key == nil {
		return nil, errors.New("not auth key setup")
	}

	ck := fmt.Sprintf("%s.%s", addr, key.Id)

	rpcClientMu.Lock()
	defer rpcClientMu.Unlock()

	if c, ok := rpcClientConns[ck]; ok {
		if forceNew {
			c.Close()
			c = nil
			delete(rpcClientConns, ck)
		} else {
			return c, nil
		}
	}

	dialOptions := []grpc.DialOption{
		grpc.WithPerRPCCredentials(newAppCredential(key)),
		grpc.WithMaxMsgSize(grpcMsgByteMax * 2),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMsgByteMax * 2)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMsgByteMax * 2)),
	}

	dialOptions = append(dialOptions, grpc.WithInsecure())

	c, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	rpcClientConns[ck] = c

	return c, nil
}
