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
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/pkg/kvapi"
	"github.com/lynkdb/kvgo/pkg/storage"
)

type jobManager struct {
	volumemu sync.RWMutex
	volumes  map[string]*ConfigVolume
}

func (it *dbServer) jobSetup() error {

	if err := it.jobTableListRefresh(); err != nil {
		return err
	}

	go it.jobRun()

	return nil
}

func (it *dbServer) jobRun() {
	tr := time.NewTimer(1e9)
	for !it.close {
		select {
		case <-tr.C:
			if err := it.jobRefresh(); err != nil {
				hlog.Printf("warn", "job refresh err %s", err.Error())
			}
			tr.Reset(1e9)
		}
	}
}

func (it *dbServer) jobRefresh() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if err := it.jobTableListRefresh(); err != nil {
		return err
	}
	return nil
}

func (it *dbServer) jobTableListRefresh() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		offset = nsSysTable("")
		cutset = nsSysTable("zzzz")
	)

	for !it.close {
		req := kvapi.NewRangeRequest(offset, cutset).SetLimit(10000)

		rs := it.dbSystem.Range(req)
		if rs.NotFound() {
			break
		} else if !rs.OK() {
			return rs.Error()
		}

		for _, item := range rs.Items {

			var tbl kvapi.Table
			if err := item.JsonDecode(&tbl); err != nil {
				return err
			}

			it.tableMapMgr.syncTable(item.Meta, &tbl)

			if it.uptime == 0 {
				hlog.Printf("info", "kvgo table %s (%d) started", tbl.Name, tbl.Id)
				// jsonPrint("table sync", tbl)
			}

			offset = item.Key
		}

		if !rs.NextResultSet {
			break
		}
	}

	offset = nsSysTableMap("")
	cutset = nsSysTableMap("zzzz")

	for !it.close {
		req := kvapi.NewRangeRequest(offset, cutset).SetLimit(10000)

		rs := it.dbSystem.Range(req)
		if rs.NotFound() {
			break
		} else if !rs.OK() {
			return rs.Error()
		}

		for _, item := range rs.Items {

			var tm kvapi.TableMap
			if err := item.JsonDecode(&tm); err != nil {
				return err
			}

			ptm := it.tableMapMgr.getById(tm.Id)
			if ptm == nil {
				continue
			}

			ptm.syncMap(item.Meta, &tm)

			// if it.uptime == 0 {
			// 	jsonPrint("table-map sync", tm)
			// }

			offset = item.Key
		}

		if !rs.NextResultSet {
			break
		}
	}

	it.tableMapMgr.iter(func(tm *tableMap) {
		if err := it.jobTableMapRefresh(tm); err != nil {
			hlog.Printf("info", "table setup err %s", err.Error())
		}
	})

	return nil
}

func (it *dbServer) jobTableMapRefresh(tm *tableMap) error {
	it.tableSetupMux.Lock()
	defer it.tableSetupMux.Unlock()

	if tm.data.Name == sysTableName {
		return nil
	}

	if len(it.cfg.Storage.Volumes) == 0 ||
		len(it.sysStatus.volumes) == 0 {
		return errors.New("storage volumes not setup")
	}

	var (
		chg  = false
		incr uint64
	)

	if tm.mapData == nil {

		r := kvapi.NewReadRequest(nsSysTableMap(tm.data.Id))
		r.Table = sysTableName

		if rs, err := it.api.Read(nil, r); err != nil {
			return err
		} else if rs.OK() {
			item := rs.Item()
			if item == nil {
				return errors.New("no data found")
			}

			var m kvapi.TableMap
			if err := item.JsonDecode(&m); err != nil {
				return err
			}

			tm.mapMeta = item.Meta
			tm.mapData = &m

		} else if rs.NotFound() {

			tm.mapMeta = &kvapi.Meta{}
			tm.mapData = &kvapi.TableMap{
				Id:      tm.data.Id,
				Version: 1,
			}
			chg = true

		} else {
			return rs.Error()
		}
	}

	if tm.mapData == nil {
		return nil
	}

	if len(tm.mapData.Shards) == 0 {
		if !chg {
			tm.mapData.Version += 1
		}
		tm.mapData.Shards, chg = []*kvapi.TableMap_Shard{
			{
				Id:      1,
				Version: tm.mapData.Version,
			},
		}, true
	}

	if tm.data.ReplicaNum < 1 {
		tm.data.ReplicaNum = 1
	} else if tm.data.ReplicaNum > 3 {
		tm.data.ReplicaNum = 3
	}

	for _, shard := range tm.mapData.Shards {
		if shard.Id > incr {
			incr = shard.Id
		}
		for _, rep := range shard.Replicas {
			if rep.Id > incr {
				incr = rep.Id
			}
		}
	}

	for _, shard := range tm.mapData.Shards {

		for len(shard.Replicas) < int(tm.data.ReplicaNum) {

			allocVolumes := map[string]bool{}
			for _, rep := range shard.Replicas {
				allocVolumes[rep.StoreId] = true
			}

			fitVolumes := []*kvapi.SysVolumeStatus{}
			for _, vs := range it.sysStatus.volumes {
				if _, ok := allocVolumes[vs.Id]; ok {
					continue
				} else {
					fitVolumes = append(fitVolumes, vs)
				}
			}

			if len(fitVolumes) > 0 {

				sort.Slice(fitVolumes, func(i, j int) bool {
					return fitVolumes[i].CapacityFree > fitVolumes[j].CapacityFree
				})

				if !chg {
					tm.mapData.Version += 1
				}
				incr += 1
				shard.Replicas, chg = append(shard.Replicas, &kvapi.TableMap_Replica{
					Id:      incr,
					StoreId: fitVolumes[0].Id,
					Version: tm.mapData.Version,
				}), true
			} else {
				break
			}
		}
	}

	if chg {
		wr := kvapi.NewWriteRequest(nsSysTableMap(tm.data.Id), jsonEncode(tm.mapData))
		wr.Table = sysTableName
		if tm.mapMeta == nil {
			wr.CreateOnly = true
		} else {
			wr.PrevVersion = tm.mapMeta.Version
		}
		rs, err := it.api.Write(nil, wr)
		if err != nil {
			return err
		}
		if !rs.OK() {
			return rs.Error()
		}

		// TODO update meta.version

		// jsonPrint("table-map reset", tm.mapData)
	}

	// jsonPrint("stores", it.stores)
	// jsonPrint("volumes", it.volumes)

	var err error

	for _, shard := range tm.mapData.Shards {

		for _, rep := range shard.Replicas {

			store := tm.getStore(rep.StoreId)
			if store != nil {
				continue
			}

			volume, ok := it.volumes[rep.StoreId]
			if !ok {
				continue
			}

			dir := fmt.Sprintf("%s/data/%s_%s", volume.Mountpoint, tm.data.Id, tm.data.Engine)
			store, err = storage.Open(tm.data.Engine, dir, nil)
			if err != nil {
				return err
			}

			tm.syncStore(rep.StoreId, store)

			hlog.Printf("info", "store setup %s ok", dir)
		}
	}

	return nil
}

func (it *dbServer) jobOnce() {
	go it.jobTableAutoClean()
	go it.jobTableLogPull(false)
}

func (it *dbServer) jobTableAutoClean() {

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		lastCleanTime = time.Now().Unix()
		tr            = time.NewTimer(1e9)
	)
	defer tr.Stop()

	for !it.close {
		tr.Reset(1e9)
		t := <-tr.C
		if lastCleanTime+5 > t.Unix() {
			continue
		}

		it.tableMapMgr.iter(func(tm *tableMap) {
			for _, rep := range tm.replicas {
				if err := rep._jobCleanTTL(); err != nil {
					hlog.Printf("error", "kvgo job clean ttl err %s", err.Error())
				}
				if err := rep._jobCleanLog(); err != nil {
					hlog.Printf("error", "kvgo job clean log err %s", err.Error())
				}
			}
		})

		lastCleanTime = time.Now().Unix()
	}
}

func (it *dbServer) jobTableLogPull(force bool) {

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		lastCleanTime = int64(0)
		tr            = time.NewTimer(1e9)
	)
	defer tr.Stop()

	for !it.close {
		tr.Reset(1e9)
		t := <-tr.C
		if !force && lastCleanTime+5 > t.Unix() {
			continue
		}
		it.tableMapMgr.iter(func(tm *tableMap) {
			if len(tm.replicas) <= 1 {
				return
			}
			for _, src := range tm.replicas {
				shard := tm.getShard(src.shardId)
				if shard == nil {
					continue
				}
				for _, dst := range tm.replicas {
					if src.replicaId == dst.replicaId {
						continue
					}
					dst._jobLogPull(shard, src)
				}
			}
		})

		lastCleanTime = time.Now().Unix()
		if force {
			break
		}
	}
}
