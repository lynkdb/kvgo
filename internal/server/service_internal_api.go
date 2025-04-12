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

	"github.com/hooto/hlog4g/hlog"
	"google.golang.org/grpc"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

type serviceApiInternalImpl struct {
	kvapi.UnimplementedKvgoInternalServer
	rpcServer      *grpc.Server
	serviceApiImpl *serviceApiImpl
	dbServer       *dbServer
}

var _ kvapi.KvgoInternalServer = &serviceApiInternalImpl{}

func (it *serviceApiInternalImpl) LogRange(
	ctx context.Context,
	breq *kvapi.LogRangeRequest,
) (*kvapi.LogRangeResponse, error) {

	tmap, rs := it.serviceApiImpl.valid(ctx, breq.Database)
	if rs != nil {
		return newLogRangeResponse(kvapi.Status_InvalidArgument, rs.StatusMessage), nil
	}

	storeIds := tmap.lookupAllStores()

	logToken := newLogRangeToken(breq.OffsetToken)
	logToken.nextApply(storeIds)

	return it.dbServer.apiInnerLogRange(breq, tmap, logToken), nil
}

func (it *serviceApiInternalImpl) Read(
	ctx context.Context,
	req *kvapi.InnerReadRequest,
) (*kvapi.ResultSet, error) {

	tmap, rs := it.serviceApiImpl.valid(ctx, req.Database)
	if rs != nil {
		return rs, nil
	}

	storeLogOffsets, err := tmap.lookupAllStoreLogOffsets()
	if err != nil {
		return nil, err
	}

	logToken := newLogRangeToken("")
	logToken.indexApply(storeLogOffsets)

	var (
		readSize  int
		limitSize = 1 << 20
	)

	rs = newResultSetOK()
	rs.NextToken = logToken.encode()

	for i, key := range req.Keys {

		hit := tmap.lookupByKey(key)
		if hit == nil || len(hit.replicas) == 0 {
			return nil, errors.New("server error")
		}

		ss := hit.replicas[0].store.Get(keyEncode(nsKeyData, key), nil)
		if ss.NotFound() {
			// TODO
			hlog.Printf("debug", "key(%s) not found", string(key))
			continue
		}

		item2, err := kvapi.KeyValueDecode(ss.Bytes())
		if err != nil {
			hlog.Printf("info", "decode key(%s) meta fail %s", string(key), err.Error())
			return nil, err
		}
		item2.Key = key

		readSize += len(ss.Bytes())
		rs.Items = append(rs.Items, item2)

		if readSize >= limitSize {
			rs.NextKeys = req.Keys[i+1:]
			break
		}
	}

	return rs, nil
}

func (it *serviceApiInternalImpl) Range(
	ctx context.Context,
	breq *kvapi.RangeRequest,
) (*kvapi.ResultSet, error) {
	return it.serviceApiImpl.Range(ctx, breq)
}
