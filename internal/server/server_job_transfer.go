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
	"bytes"
	"errors"
	"sync"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

type transferManager struct {
	mu   sync.Mutex
	jobs map[string]*transferJobEntry
}

type transferJobEntry struct {
	job      *ConfigTransferJob   `json:"job"`
	inOffset *JobTransferInOffset `json:"status"`
}

func newTransferManager() *transferManager {
	return &transferManager{
		jobs: map[string]*transferJobEntry{},
	}
}

func (it *transferManager) setJobEntry(job *transferJobEntry) *transferJobEntry {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.jobs == nil {
		it.jobs = map[string]*transferJobEntry{}
	}
	it.jobs[job.job.UniId] = job
	return job
}

func (it *transferManager) jobEntry(uid string) *transferJobEntry {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.jobs == nil {
		it.jobs = map[string]*transferJobEntry{}
	}
	if job, ok := it.jobs[uid]; ok {
		return job
	}
	return nil
}

func (it *transferManager) iter(fn func(jobEntry *transferJobEntry)) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if len(it.jobs) > 0 {
		for _, job := range it.jobs {
			fn(job)
		}
	}
}

func (it *dbServer) jobDatabaseTransferSetup() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	var (
		tn = timesec()
	)

	batchPull := func(
		job *ConfigTransferJob,
		jobOffset *JobTransferInOffset,
		logMetas map[string]*kvapi.LogMeta,
		client *internalClientConn, dmSink *dbMap) error {

		if len(logMetas) == 0 {
			return nil
		}

		for _, logMeta := range logMetas {

			if !kvapi.AttrAllow(logMeta.Attrs, kvapi.Write_Attrs_Delete) {
				continue
			}

			lrs, err := it.api.Read(nil, &kvapi.ReadRequest{
				Database: job.SinkDatabase,
				Keys:     [][]byte{logMeta.Key},
				Attrs:    kvapi.Read_Attrs_MetaOnly,
			})
			if err != nil {
				return err
			}
			if lrs.NotFound() {
				continue
			} else if !lrs.OK() {
				return lrs.Error()
			}

			if locMeta := lrs.Meta(); locMeta == nil || logMeta.Created < locMeta.Updated {
				continue
			}

			lrs, err = it.api.Delete(nil, &kvapi.DeleteRequest{
				Database: job.SinkDatabase,
				Key:      logMeta.Key,
				Attrs:    kvapi.Write_Attrs_InnerSync,
			})
			if err != nil {
				return err
			}
			if !lrs.OK() {
				return lrs.Error()
			}

			jobOffset.Stats.LocalDelete += 1
		}

		var (
			locMetas = map[string]*kvapi.Meta{}

			dataReq = &kvapi.InnerReadRequest{
				Database: job.Source.Database,
			}
		)

		for _, logMeta := range logMetas {

			if kvapi.AttrAllow(logMeta.Attrs, kvapi.Write_Attrs_Delete) {
				continue
			}

			lrs, err := it.api.Read(nil, &kvapi.ReadRequest{
				Database: job.SinkDatabase,
				Keys:     [][]byte{logMeta.Key},
				Attrs:    kvapi.Read_Attrs_MetaOnly,
			})
			if err != nil {
				return err
			}

			jobOffset.Stats.LocalMetaRead += 1

			if lrs.NotFound() {

			} else if !lrs.OK() {
				return lrs.Error()
			} else {

				meta := lrs.Meta()
				if meta != nil && meta.Version > 0 {

					if meta.Checksum == logMeta.Checksum {
						jobOffset.Stats.LocalMetaSkip += 1
						continue
					}

					if meta.Updated == logMeta.Created {
						jobOffset.Stats.MergeSkip += 1
					} else if meta.Updated > logMeta.Created {
						jobOffset.Stats.MergeDeny += 1
					} else {
						locMetas[string(logMeta.Key)] = meta
					}
				}
			}

			dataReq.Keys = append(dataReq.Keys, logMeta.Key)
		}

		for !it.close && len(dataReq.Keys) > 0 {

			rs2 := client.innerRead(dataReq)
			if rs2.NotFound() {
				//
			} else if !rs2.OK() {
				hlog.Printf("info", "transfer fail %s", rs2.ErrorMessage())
				return rs2.Error()
			}

			// hlog.Printf("info", "transfer data pull keys %d, next-keys %d",
			// 	len(rs2.Items), len(rs2.NextKeys))

			if jobOffset.FullScan {
				jobOffset.Stats.FullDataRead += int64(len(rs2.Items))
			} else {
				jobOffset.Stats.DeltaDataRead += int64(len(rs2.Items))
			}

			for _, item := range rs2.Items {

				pLogMeta, ok := logMetas[string(item.Key)]
				if !ok {
					continue
				}

				if item.Meta.Version != pLogMeta.Version {
					continue
				}

				if locMeta, ok := locMetas[string(item.Key)]; ok && locMeta.Updated >= item.Meta.Updated {
					if locMeta.Updated == item.Meta.Updated {
						jobOffset.Stats.MergeSkip += 1
					} else if locMeta.Updated > item.Meta.Updated {
						jobOffset.Stats.MergeDeny += 1
					}
					continue
				}

				item.Meta.Version = 0

				writeRequest := &kvapi.WriteRequest{
					Database: job.SinkDatabase,
					Meta:     item.Meta,
					Key:      item.Key,
					Value:    item.Value,
					Attrs:    kvapi.Write_Attrs_InnerSync,
				}
				if item.Meta.IncrId > 0 {
					testPrintf("pull incr key %s, incr %d", string(item.Key), item.Meta.IncrId)
				}
				if rs0, err := it.api.Write(nil, writeRequest); err != nil {
					return err
				} else if !rs0.OK() {
					return rs0.Error()
				}

				if jobOffset.FullScan {
					jobOffset.Stats.FullDataFlush += 1
				} else {
					jobOffset.Stats.DeltaDataFlush += 1
				}
			}

			dataReq.Keys = rs2.NextKeys
		}

		return nil
	}

	jobAction := func(job *ConfigTransferJob, client *internalClientConn, dmSink *dbMap) error {

		it.jobSetupMut.Lock()
		defer it.jobSetupMut.Unlock()

		var (
			jobStorKey = nsSysTransferJob(job.UniId)
			jobEntry   = it.transferMgr.jobEntry(job.UniId)
		)

		if jobEntry == nil {

			var obj JobTransferInOffset

			if ss := it.dbSystem.store.Get(jobStorKey, nil); ss.NotFound() {
				//
			} else if !ss.OK() {
				return ss.Error()
			} else if err := jsonDecode(ss.Bytes(), &obj); err != nil {
				return err
			} else {
				hlog.Printf("info", "job-transfer load state %s", string(ss.Bytes()))
			}

			jobEntry = &transferJobEntry{
				job:      job,
				inOffset: &obj,
			}

			obj.UniId = job.UniId
			it.transferMgr.setJobEntry(jobEntry)
			// jobEntry.inOffset.LogToken = ""
			// jobEntry.inOffset.Updated = 0
		}

		if jobEntry.inOffset.Updated+job.RepeatIntervalSeconds > tn {
			return nil
		}

		prevDataFlush := jobEntry.inOffset.Stats.FullDataFlush +
			jobEntry.inOffset.Stats.DeltaDataFlush

		for !it.close && !jobEntry.inOffset.FullScan {

			req := &kvapi.LogRangeRequest{
				Database:    job.Source.Database,
				OffsetToken: jobEntry.inOffset.LogToken,
			}

			rs := client.innerLogRange(req)

			// hlog.Printf("info", "internal log-range req token %v, resp token %v, out-range %v",
			// 	jobEntry.inOffset.LogToken, rs.NextOffsetToken, rs.LogOffsetOutrange)

			if !rs.OK() {
				if !rs.NotFound() {
					hlog.Printf("info", "internal log-range fail %s", rs.Status.Message)
				}
				break
			}

			if rs.NextOffsetToken == "" {
				return errors.New("server-error: token")
			}

			if rs.LogOffsetOutrange {
				jobEntry.inOffset.LogToken = rs.NextOffsetToken
				jobEntry.inOffset.FullScan = true
				jobEntry.inOffset.KeyOffset = nil
				hlog.Printf("info", "internal log-range out-range %s", string(jsonEncode(jobEntry.inOffset)))
				break
			}

			if len(rs.Items) == 0 {
				break
			}

			// hlog.Printf("info", "log-pull count %d", len(rs.Items))

			var logMetas = map[string]*kvapi.LogMeta{}
			for _, logMeta := range rs.Items {
				if meta, ok := logMetas[string(logMeta.Key)]; !ok || logMeta.Version > meta.Version {
					logMetas[string(logMeta.Key)] = logMeta
				}
			}

			jobEntry.inOffset.Stats.DeltaLogRead += int64(len(rs.Items))

			if err := batchPull(job, jobEntry.inOffset, logMetas, client, dmSink); err != nil {
				return err
			}

			jobEntry.inOffset.LogToken = rs.NextOffsetToken

			if ss := it.dbSystem.store.Put(jobStorKey, jsonEncode(jobEntry.inOffset), nil); !ss.OK() {
				return ss.Error()
			}
		}

		if jobEntry.inOffset.FullScan {
			jobEntry.inOffset.Stats.FullPull += 1
		}

		for rn := int64(1); !it.close && jobEntry.inOffset.FullScan; rn++ {

			var (
				req = &kvapi.RangeRequest{
					Database: job.Source.Database,
					LowerKey: jobEntry.inOffset.KeyOffset,
					UpperKey: bytes.Repeat([]byte{0xff}, 128),
					Limit:    1000,
					Attrs:    kvapi.Read_Attrs_MetaOnly,
				}

				rs = client.innerRange(req)

				logMetas = map[string]*kvapi.LogMeta{}
			)

			jobEntry.inOffset.Stats.FullMetaRead += int64(len(rs.Items))
			// hlog.Printf("info", "full-pull meta count %d", len(rs.Items))

			for _, item := range rs.Items {

				logMeta := &kvapi.LogMeta{
					Key:      item.Key,
					Version:  item.Meta.Version,
					Created:  item.Meta.Updated,
					Checksum: item.Meta.Checksum,
					Attrs:    item.Meta.Attrs,
				}

				logMetas[string(item.Key)] = logMeta
			}

			if err := batchPull(job, jobEntry.inOffset, logMetas, client, dmSink); err != nil {
				return err
			}

			if !it.close {
				if len(rs.Items) > 0 {
					jobEntry.inOffset.KeyOffset = rs.Items[len(rs.Items)-1].Key
				} else {
					jobEntry.inOffset.FullScan = false
					jobEntry.inOffset.KeyOffset = nil
				}
			}

			if ss := it.dbSystem.store.Put(jobStorKey, jsonEncode(jobEntry.inOffset), nil); !ss.OK() {
				return ss.Error()
			}
		}

		jobEntry.inOffset.Updated = tn

		if n := (jobEntry.inOffset.Stats.FullDataFlush + jobEntry.inOffset.Stats.DeltaDataFlush) - prevDataFlush; n > 0 {
			hlog.Printf("info", "job-transfer %s, flush %d, offsets %s", job.UniId, n, string(jsonEncode(jobEntry.inOffset)))
		}

		return nil
	}

	for _, jobEntry := range it.cfg.TransferJobs {

		if jobEntry.UniId == "" || jobEntry.Action != "enable" {
			continue
		}

		dmSink := it.dbMapMgr.getByName(jobEntry.SinkDatabase)
		if dmSink == nil {
			continue
		}

		c, err := jobEntry.Source.newClient()
		if err != nil {
			hlog.Printf("info", "jobEntryansfer source %s, connect fail %s", jobEntry.Source.Addr, err.Error())
			continue
		}

		jobAction(jobEntry, c, dmSink)
	}

	return nil
}
