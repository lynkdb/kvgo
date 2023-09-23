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

	"github.com/hooto/hlog4g/hlog"
	"google.golang.org/grpc"

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

type serviceAdminImpl struct {
	kvapi.UnimplementedKvgoAdminServer
	rpcServer *grpc.Server
	dbServer  *dbServer
}

var _ kvapi.KvgoAdminServer = &serviceAdminImpl{}

func (it *serviceAdminImpl) TableList(
	ctx context.Context,
	req *kvapi.TableListRequest,
) (*kvapi.ResultSet, error) {

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	rs := newResultSetOK()

	it.dbServer.tableMapMgr.iter(func(tbl *tableMap) {
		if tbl.data != nil {
			resultSetAppendWithJsonObject(rs, []byte(tbl.data.Name), tbl.meta, tbl.data)
		}
	})

	return rs, nil
}

func (it *serviceAdminImpl) TableCreate(
	ctx context.Context,
	req *kvapi.TableCreateRequest,
) (*kvapi.ResultSet, error) {

	if !kvapi.TableNameRX.MatchString(req.Name) ||
		req.Name == sysTableName {
		return newResultSetWithClientError("invalid table name"), nil
	}

	if req.Engine != storage.DefaultDriver {
		return newResultSetWithClientError("invalid table engine " + req.Engine), nil
	}

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	if tbl := it.dbServer.tableMapMgr.getByName(req.Name); tbl != nil {
		return newResultSetWithConflict("table already exist " + req.Name), nil
	}

	if req.ReplicaNum < 1 {
		req.ReplicaNum = 1
	} else if req.ReplicaNum > maxReplicaCap {
		req.ReplicaNum = maxReplicaCap
	}

	tbl := &kvapi.Table{
		Id:         randHexString(16),
		Name:       req.Name,
		Engine:     req.Engine,
		ReplicaNum: req.ReplicaNum,
	}

	wr := kvapi.NewWriteRequest(nsSysTable(tbl.Id), jsonEncode(tbl))
	wr.Table = sysTableName
	wr.CreateOnly = true
	wr.Attrs |= kvapi.Write_Attrs_Sync

	ss, err := it.dbServer.api.Write(ctx, wr)
	if err != nil {
		return newResultSetWithServerError(err.Error()), nil
	}
	if !ss.OK() {
		return newResultSetWithServerError(ss.Error().Error()), nil
	}

	testPrintf("server %d, table create %s", it.dbServer.pid, req.Name)
	tm := it.dbServer.tableMapMgr.syncTable(ss.Meta(), tbl)

	if it.dbServer.cfg.Server.IsStandaloneMode() {
		it.dbServer.jobTableMapRefresh(tm)
	}

	hlog.Printf("info", "table create : %v", tbl)
	// jsonPrint("table create", tbl)

	rs := newResultSetOK()
	resultSetAppendWithJsonObject(rs, []byte(req.Name), ss.Meta(), tbl)

	return rs, nil
}

func (it *serviceAdminImpl) TableAlter(
	ctx context.Context,
	req *kvapi.TableAlterRequest,
) (*kvapi.ResultSet, error) {

	if !kvapi.TableNameRX.MatchString(req.Name) ||
		req.Name == sysTableName {
		return newResultSetWithClientError("invalid table name"), nil
	}

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	tmap := it.dbServer.tableMapMgr.getByName(req.Name)
	if tmap == nil || tmap.meta == nil {
		return newResultSetWithNotFound("table not found"), nil
	}

	chg := false

	if req.ReplicaNum > 0 {
		if req.ReplicaNum > maxReplicaCap {
			return newResultSetWithClientError("invalid replica num (1 <= n <= 3)"), nil
		}
		if req.ReplicaNum > tmap.data.ReplicaNum {
			tmap.data.ReplicaNum, chg = req.ReplicaNum, true
		}
	}

	rs := newResultSetOK()

	if chg {
		wr := kvapi.NewWriteRequest(nsSysTable(tmap.data.Id), jsonEncode(tmap.data))
		wr.Table = sysTableName
		wr.PrevVersion = tmap.meta.Version
		wr.Attrs |= kvapi.Write_Attrs_Sync

		rsTable, err := it.dbServer.api.Write(ctx, wr)
		if err != nil {
			return newResultSetWithServerError(err.Error()), nil
		}

		if !rsTable.OK() {
			return newResultSetWithServerError(rsTable.Error().Error()), nil
		}

		testPrintf("server %d, table create %s", it.dbServer.pid, req.Name)

		it.dbServer.tableMapMgr.syncTable(rsTable.Meta(), tmap.data)
		resultSetAppendWithJsonObject(rs, []byte(req.Name), rsTable.Meta(), tmap.data)
	} else {
		resultSetAppendWithJsonObject(rs, []byte(req.Name), tmap.meta, tmap.data)
	}

	hlog.Printf("info", "table alter : %v", tmap.data)
	// jsonPrint("table alter", tmap.data)

	return rs, nil
}

func (it *serviceAdminImpl) auth(ctx context.Context) error {

	if ctx != nil {

		av, err := appAuthParse(ctx, it.dbServer.keyMgr)
		if err != nil {
			return err
		}

		if err := av.SignValid(nil); err != nil {
			return err
		}

		if err := av.Allow(authPermSysAll); err != nil {
			return err
		}
	}

	return nil
}
