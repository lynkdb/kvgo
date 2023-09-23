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

	"google.golang.org/grpc"

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

type serviceApiImpl struct {
	kvapi.UnimplementedKvgoServer
	rpcServer *grpc.Server
	dbServer  *dbServer
}

var _ kvapi.KvgoServer = &serviceApiImpl{}

func (it *serviceApiImpl) Write(
	ctx context.Context,
	req *kvapi.WriteRequest,
) (*kvapi.ResultSet, error) {

	tmap, rs := it.valid(ctx, req.Table)
	if rs != nil {
		return rs, nil
	}

	hit := tmap.lookupByKey(req.Key)
	if hit.replicaNum == 1 && len(hit.replicas) > 0 {
		return hit.replicas[0].Write(req), nil
	} else if hit.replicaNum > 1 {
		return it.dbServer.apiWrite(req, hit), nil
	}

	return newResultSetWithServerError("write: server not ready"), nil
}

func (it *serviceApiImpl) Delete(
	ctx context.Context,
	req *kvapi.DeleteRequest,
) (*kvapi.ResultSet, error) {

	tmap, rs := it.valid(ctx, req.Table)
	if rs != nil {
		return rs, nil
	}

	hit := tmap.lookupByKey(req.Key)
	if hit.replicaNum == 1 && len(hit.replicas) > 0 {
		return hit.replicas[0].Delete(req), nil
	} else if hit.replicaNum > 1 {
		return it.dbServer.apiDelete(req, hit), nil
	}

	return newResultSetWithServerError("server not ready"), nil
}

func (it *serviceApiImpl) Read(
	ctx context.Context,
	req *kvapi.ReadRequest,
) (*kvapi.ResultSet, error) {

	tmap, rs := it.valid(ctx, req.Table)
	if rs != nil {
		return rs, nil
	}

	hits := tmap.lookupByKeys(req.Keys)
	if len(hits) == 1 && hits[0].replicaNum == 1 && len(hits[0].replicas) > 0 {
		return hits[0].replicas[0].Read(req), nil
	} else if len(hits) > 0 {
		return it.dbServer.apiRead(req, hits), nil
	}

	return newResultSetWithServerError("server not ready"), nil
}

func (it *serviceApiImpl) Range(
	ctx context.Context,
	req *kvapi.RangeRequest,
) (*kvapi.ResultSet, error) {

	tmap, rs := it.valid(ctx, req.Table)
	if rs != nil {
		return rs, nil
	}

	hits := tmap.lookupByRange(req.LowerKey, req.UpperKey, req.Revert)
	if len(hits) == 1 && hits[0].replicaNum == 1 && len(hits[0].replicas) > 0 {
		return hits[0].replicas[0].Range(req), nil
	} else if len(hits) > 0 {
		return it.dbServer.apiRange(req, hits), nil
	}

	return newResultSetWithServerError("server not ready"), nil
}

func (it *serviceApiImpl) Batch(
	ctx context.Context,
	breq *kvapi.BatchRequest,
) (*kvapi.BatchResponse, error) {

	if err := breq.Valid(); err != nil {
		return newBatchResponse(kvapi.Status_InvalidArgument, err.Error()), nil
	}

	if _, rs := it.valid(ctx, breq.Table); rs != nil {
		return newBatchResponse(kvapi.Status_InvalidArgument, rs.StatusMessage), nil
	}

	var brs kvapi.BatchResponse

	ok := 0

	for _, req := range breq.Items {
		var rs *kvapi.ResultSet
		switch req.Value.(type) {
		case *kvapi.RequestUnion_Write:
			rs, _ = it.Write(nil, req.Value.(*kvapi.RequestUnion_Write).Write)

		case *kvapi.RequestUnion_Delete:
			rs, _ = it.Delete(nil, req.Value.(*kvapi.RequestUnion_Delete).Delete)

		case *kvapi.RequestUnion_Read:
			rs, _ = it.Read(nil, req.Value.(*kvapi.RequestUnion_Read).Read)

		case *kvapi.RequestUnion_Range:
			rs, _ = it.Range(nil, req.Value.(*kvapi.RequestUnion_Range).Range)

		default:
			rs = newResultSetWithClientError("invalid request type #service-api")
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

	return &brs, nil
}

func (it *serviceApiImpl) valid(ctx context.Context, tableName string) (*tableMap, *kvapi.ResultSet) {

	if !kvapi.TableNameRX.MatchString(tableName) {
		return nil, newResultSetWithClientError("invalid table name " + tableName)
	}

	if s := it.auth(ctx); s != nil {
		return nil, s
	}

	tbl := it.dbServer.tableMapMgr.getByName(tableName)
	if tbl == nil {
		return nil, newResultSet(kvapi.Status_NotFound, "table not found")
	}

	return tbl, nil
}

func (it *serviceApiImpl) auth(ctx context.Context) *kvapi.ResultSet {

	if ctx != nil {

		av, err := appAuthParse(ctx, it.dbServer.keyMgr)
		if err != nil {
			return newResultSet(kvapi.Status_AuthDeny, err.Error())
		}

		if err := av.SignValid(nil); err != nil {
			return newResultSet(kvapi.Status_AuthDeny, err.Error())
		}
	}

	return nil
}
