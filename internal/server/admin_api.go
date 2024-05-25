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
	"sort"
	"strings"

	"github.com/hooto/hlog4g/hlog"
	"google.golang.org/grpc"

	"github.com/lynkdb/lynkx/datax"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

type serviceAdminImpl struct {
	kvapi.UnimplementedKvgoAdminServer
	rpcServer *grpc.Server
	dbServer  *dbServer
}

var _ kvapi.KvgoAdminServer = &serviceAdminImpl{}

func (it *serviceAdminImpl) DatabaseList(
	ctx context.Context,
	req *kvapi.DatabaseListRequest,
) (*kvapi.ResultSet, error) {

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	rs := newResultSetOK()

	it.dbServer.dbMapMgr.iter(func(tbl *dbMap) {
		if tbl.data != nil {
			resultSetAppendWithJsonObject(rs, []byte(tbl.data.Name), tbl.meta, tbl.data)
		}
	})

	return rs, nil
}

func (it *serviceAdminImpl) DatabaseCreate(
	ctx context.Context,
	req *kvapi.DatabaseCreateRequest,
) (*kvapi.ResultSet, error) {

	if !kvapi.DatabaseNameRX.MatchString(req.Name) ||
		req.Name == sysDatabaseName {
		return newResultSetWithClientError("invalid database name"), nil
	}

	if req.Engine != "" && req.Engine != storage.DefaultDriver {
		return newResultSetWithClientError("invalid database engine " + req.Engine), nil
	} else if req.Engine == "" {
		req.Engine = storage.DefaultDriver
	}

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	if tbl := it.dbServer.dbMapMgr.getByName(req.Name); tbl != nil {
		return newResultSetWithConflict("database already exist " + req.Name), nil
	}

	if req.ReplicaNum < minReplicaCap {
		req.ReplicaNum = minReplicaCap
	} else if req.ReplicaNum > maxReplicaCap {
		req.ReplicaNum = maxReplicaCap
	}

	tbl := &kvapi.Database{
		Id:         randHexString(8),
		Name:       req.Name,
		Engine:     req.Engine,
		ReplicaNum: req.ReplicaNum,
		Desc:       req.Desc,
	}

	if req.ShardSize >= kShardSplit_CapacitySize_Min &&
		req.ShardSize <= kShardSplit_CapacitySize_Max {
		tbl.ShardSize = req.ShardSize
	} else {
		tbl.ShardSize = kShardSplit_CapacitySize_Def
	}

	wr := kvapi.NewWriteRequest(nsSysDatabaseSpec(tbl.Id), jsonEncode(tbl))
	wr.Database = sysDatabaseName
	wr.CreateOnly = true
	wr.Attrs |= kvapi.Write_Attrs_Sync

	ss, err := it.dbServer.api.Write(ctx, wr)
	if err != nil {
		return newResultSetWithServerError(err.Error()), nil
	}
	if !ss.OK() {
		return newResultSetWithServerError(ss.Error().Error()), nil
	}

	it.dbServer.auditLogger.Put("admin-api", "database %s, engine %s, replica-num %d, created",
		tbl.Name, tbl.Engine, tbl.ReplicaNum)

	tm := it.dbServer.dbMapMgr.syncDatabase(ss.Meta(), tbl)

	if it.dbServer.cfg.Server.IsStandaloneMode() {
		it.dbServer._jobDatabaseMapSetup(tm)
	}

	hlog.Printf("info", "database create : %v", tbl)

	rs := newResultSetOK()
	resultSetAppendWithJsonObject(rs, []byte(req.Name), ss.Meta(), tbl)

	return rs, nil
}

func (it *serviceAdminImpl) DatabaseUpdate(
	ctx context.Context,
	req *kvapi.DatabaseUpdateRequest,
) (*kvapi.ResultSet, error) {

	if !kvapi.DatabaseNameRX.MatchString(req.Name) ||
		req.Name == sysDatabaseName {
		return newResultSetWithClientError("invalid database name"), nil
	}

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	tmap := it.dbServer.dbMapMgr.getByName(req.Name)
	if tmap == nil || tmap.meta == nil {
		return newResultSetWithNotFound("database not found"), nil
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

	if req.ShardSize > 0 {
		if req.ShardSize < kShardSplit_CapacitySize_Min ||
			req.ShardSize > kShardSplit_CapacitySize_Max {
			return newResultSetWithClientError("invalid shard size"), nil
		}
		if req.ShardSize != tmap.data.ShardSize {
			tmap.data.ShardSize, chg = req.ShardSize, true
		}
	}

	req.Desc = strings.TrimSpace(req.Desc)
	if req.Desc != "" && req.Desc != tmap.data.Desc {
		tmap.data.Desc, chg = req.Desc, true
	}

	rs := newResultSetOK()

	if chg {
		wr := kvapi.NewWriteRequest(nsSysDatabaseSpec(tmap.data.Id), jsonEncode(tmap.data))
		wr.Database = sysDatabaseName
		wr.PrevVersion = tmap.meta.Version
		wr.Attrs |= kvapi.Write_Attrs_Sync

		rsDatabase, err := it.dbServer.api.Write(ctx, wr)
		if err != nil {
			return newResultSetWithServerError(err.Error()), nil
		}

		if !rsDatabase.OK() {
			return newResultSetWithServerError(rsDatabase.Error().Error()), nil
		}

		it.dbServer.auditLogger.Put("admin-api", "database %s, replica-num %d, updated",
			tmap.data.Name, tmap.data.ReplicaNum)

		it.dbServer.dbMapMgr.syncDatabase(rsDatabase.Meta(), tmap.data)
		resultSetAppendWithJsonObject(rs, []byte(req.Name), rsDatabase.Meta(), tmap.data)
	} else {
		resultSetAppendWithJsonObject(rs, []byte(req.Name), tmap.meta, tmap.data)
	}

	hlog.Printf("info", "database update : %v", tmap.data)

	return rs, nil
}

func (it *serviceAdminImpl) SysGet(
	ctx context.Context,
	req *kvapi.SysGetRequest,
) (*kvapi.ResultSet, error) {

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return newResultSetWithServerError("runtime mode not setup"), nil
	}

	if err := it.auth(ctx); err != nil {
		return newResultSetWithAuthDeny(err.Error()), nil
	}

	if len(req.Name) == 0 {
		return newResultSetWithClientError("name not found"), nil
	}

	if req.Params == nil {
		req.Params = map[string]string{}
	}

	if req.Limit == 0 {
		req.Limit = 10
	} else if req.Limit <= 0 {
		req.Limit = 1
	} else if req.Limit > 1000 {
		req.Limit = 1000
	}

	rs := newResultSetOK()

	switch req.Name {

	case "info":

		resultSetAppendWithJsonObject(rs, []byte("server/config"), &kvapi.Meta{}, it.dbServer.cfg)

		resultSetAppendWithJsonObject(rs, []byte("server/status"), &kvapi.Meta{}, it.dbServer.status)

		resultSetAppendWithJsonObject(rs, []byte("store/status"), &kvapi.Meta{}, it.dbServer.storeMgr.status)

		if _, ok := req.Params["all"]; ok {
			it.dbServer.dbMapMgr.iter(func(tm *dbMap) {
				resultSetAppendWithJsonObject(rs, []byte("db/"+tm.data.Name+"/spec"), &kvapi.Meta{}, tm.data)
				resultSetAppendWithJsonObject(rs, []byte("db/"+tm.data.Name+"/map"), &kvapi.Meta{}, tm.mapData)
				tm.status.IncrOffsets = tm.incrMgr.Items
				resultSetAppendWithJsonObject(rs, []byte("db/"+tm.data.Name+"/status"), &kvapi.Meta{}, &tm.status)
			})
		}

	case "auditlog":
		ds := it.dbServer.dbSystem.Range(&kvapi.RangeRequest{
			LowerKey: nsSysAuditLog(false),
			UpperKey: nsSysAuditLog(true),
			Revert:   true,
			Limit:    req.Limit,
		})
		if ds.OK() {
			for _, item := range ds.Items {
				resultSetAppend(rs, []byte(string(item.Key)), &kvapi.Meta{}, item.Value)
			}
		}

	case "db-info":

		if len(req.Params) == 0 {
			return newResultSetWithClientError("db name not found"), nil
		}

		dbName, ok := req.Params["db_name"]
		if !ok {
			return newResultSetWithClientError("db name not found"), nil
		}

		it.dbServer.dbMapMgr.iter(func(tm *dbMap) {
			if tm.data.Name != dbName {
				return
			}
			resultSetAppendWithJsonObject(rs, []byte("db/"+tm.data.Name+"/map"), &kvapi.Meta{}, tm.mapData)
			tm.status.IncrOffsets = tm.incrMgr.Items
			resultSetAppendWithJsonObject(rs, []byte("db/"+tm.data.Name+"/status"), &kvapi.Meta{}, &tm.status)
		})

		if len(rs.Items) == 0 {
			return newResultSetWithClientError("no database"), nil
		}

	case "store-info":
		sort.Slice(it.dbServer.storeMgr.status.Items, func(i, j int) bool {
			return it.dbServer.storeMgr.status.Items[i].CapacityFree > it.dbServer.storeMgr.status.Items[j].CapacityFree
		})
		resultSetAppendWithJsonObject(rs, []byte("store/status"), &kvapi.Meta{}, it.dbServer.storeMgr.status)

	default:
		return nil, fmt.Errorf("name (%s) not match", req.Name)
	}

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

type AdminService struct {
	dbServer *dbServer
}

func (it *AdminService) PreMethod(ctx context.Context) error {

	if !it.dbServer.cfg.Server.IsStandaloneMode() {
		return errors.New("runtime mode not setup")
	}

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

type JobListRequest struct{}

type JobListResponse struct {
	Items []*JobItem `json:"items" x_attrs:"rows"`
}

type JobItem struct {
	Job    *ConfigTransferJob   `json:"job"`
	Offset *JobTransferInOffset `json:"offset"`
}

func (it *AdminService) JobList(
	ctx datax.Context,
	req *JobListRequest,
) (*JobListResponse, error) {

	rs := &JobListResponse{}

	it.dbServer.transferMgr.iter(func(jobEntry *transferJobEntry) {
		rs.Items = append(rs.Items, &JobItem{
			Job:    jobEntry.job,
			Offset: jobEntry.inOffset,
		})
	})

	return rs, nil
}

type JobUpdateTransferRequest struct {
	UniId                 string                  `json:"uni_id" x_attrs:"primary_key"`
	Action                string                  `json:"action" x_attrs:"create_required" x_enums:"enable,disable" x_value_limits:"enable"`
	Source                JobUpdateTransferSource `json:"source" x_attrs:"create_required"`
	SinkDatabase          string                  `json:"sink_database" x_attrs:"create_required"`
	RepeatIntervalSeconds int64                   `json:"repeat_interval_seconds" x_value_limits:"60,60,3600"`
	Desc                  string                  `json:"desc,omitempty"`
}

type JobUpdateTransferAccessKey struct {
	Id     string `json:"id" x_attrs:"create_required"`
	Secret string `json:"secret" x_attrs:"create_required"`
}

type JobUpdateTransferSource struct {
	Addr      string                     `json:"addr" x_attrs:"create_required"`
	Database  string                     `json:"database" x_attrs:"create_required"`
	AccessKey JobUpdateTransferAccessKey `json:"access_key" x_attrs:"create_required"`
	Desc      string                     `json:"desc,omitempty"`
}

func (it *AdminService) JobUpdate(
	ctx datax.Context,
	req *JobUpdateTransferRequest,
) (*ConfigTransferJob, error) {

	jobEntry := it.dbServer.transferMgr.jobEntry(req.UniId)
	if jobEntry == nil || jobEntry.job == nil {
		return nil, datax.NewNotFoundError(fmt.Sprintf("job (%s) not found", req.UniId))
	}

	if chg, err := ctx.RequestSpec().DataMerge(jobEntry.job, req); err != nil {
		return nil, datax.NewBadRequestError(fmt.Sprintf("data merge fail : %s", err.Error()))
	} else if chg {
		it.dbServer.ConfigFlush()
	}

	return jobEntry.job, nil
}

func (it *AdminService) DatabaseList(
	ctx datax.Context,
	req *kvapi.DatabaseListRequest,
) (*kvapi.ResultSet, error) {

	rs := newResultSetOK()

	it.dbServer.dbMapMgr.iter(func(tbl *dbMap) {
		if tbl.data != nil {
			resultSetAppendWithJsonObject(rs, []byte(tbl.data.Name), tbl.meta, tbl.data)
		}
	})

	return rs, nil
}
