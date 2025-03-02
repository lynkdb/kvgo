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
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hooto/hlog4g/hlog"
	ps_cpu "github.com/shirou/gopsutil/v3/cpu"
	ps_disk "github.com/shirou/gopsutil/v3/disk"
	ps_mem "github.com/shirou/gopsutil/v3/mem"
	ps_net "github.com/shirou/gopsutil/v3/net"
	ps_proc "github.com/shirou/gopsutil/v3/process"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func (it *dbServer) taskRun() {
	tr := time.NewTimer(1e9)
	for !it.close {
		select {
		case <-tr.C:
			if err := it.taskRefresh(); err != nil {
				hlog.Printf("warn", "task refresh err %s", err.Error())
			}
			tr.Reset(1e9)
		}
	}
}

func (it *dbServer) taskRefresh() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if err := it.taskReplicaListRefresh(false); err != nil {
		return err
	}
	if err := it.taskSystemStatusRefresh(); err != nil {
		return err
	}
	return nil
}

func (it *dbServer) taskReplicaListRefresh(forceRefresh bool) error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	it.dbMapMgr.iter(func(tm *dbMap) {

		//
		if tm.meta == nil || tm.data == nil || tm.data.ReplicaNum < 1 ||
			tm.mapMeta == nil || tm.mapData == nil {
			return
		}

		//
		it._task_statusRefresh(tm, tm.mapData, forceRefresh)

		//
		it._task_replicaRemove(tm, tm.mapData)
	})

	return nil
}

func (it *dbServer) _task_statusRefresh(tm *dbMap, mapData *kvapi.DatabaseMap, forceRefresh bool) {

	for i := 0; i < len(mapData.Shards); i++ {

		var (
			shard    = mapData.Shards[i]
			lowerKey = bytesClone(shard.LowerKey)
			upperKey []byte
		)

		for j := i + 1; j < len(mapData.Shards); j++ {
			if kvapi.AttrAllow(mapData.Shards[j].Action, kShardSetup_In) {
				upperKey = bytesClone(mapData.Shards[j].LowerKey)
				break
			}
		}
		if len(upperKey) == 0 {
			upperKey = []byte{0xff}
		}

		for _, rep := range shard.Replicas {
			repInst := tm.tryInitReplica(shard, rep)
			if repInst == nil {
				continue
			}
			repInst.taskStatusRefresh(tm, shard, lowerKey, upperKey, forceRefresh)
		}
	}
}

func (it *dbServer) _task_replicaRemove(tm *dbMap, mapData *kvapi.DatabaseMap) {

	for i := 0; i < len(mapData.Shards); i++ {

		var shard = mapData.Shards[i]

		if len(shard.Replicas) <= int(tm.data.ReplicaNum) {
			continue
		}

		var (
			lowerKey = bytesClone(shard.LowerKey)
			upperKey []byte
		)

		if i+1 < len(mapData.Shards) {
			upperKey = bytesClone(mapData.Shards[i+1].LowerKey)
		} else {
			upperKey = []byte{0xff}
		}

		for _, rep := range shard.Replicas {
			//
			if !kvapi.AttrAllow(rep.Action, kReplicaSetup_Remove) {
				continue
			}

			//
			repInst := tm.tryInitReplica(shard, rep)
			if repInst == nil {
				continue
			}

			if kvapi.AttrAllow(repInst.localStatus.action.attr, kReplicaStatus_Remove) {
				continue
			}

			if err := repInst._task_deleteRange(it, shard, lowerKey, upperKey); err == nil {
				repInst.localStatus.action.attr = kReplicaStatus_Remove
				tm.tryRemoveReplica(rep.Id)
			}
		}
	}
}

var (
	diskDevSet       []ps_disk.PartitionStat
	diskStatusCaches = map[string]ps_disk.IOCountersStat{}
)

func (it *dbServer) taskSystemStatusRefresh() error {

	if it.close {
		return nil
	}

	it.closegw.Add(1)
	defer func() {
		it.closegw.Done()
	}()

	tn := time.Now().Unix()
	if (it.localSysStatus.Updated + kSysStatus_TaskIntervalSeconds) > tn {
		return nil
	}
	defer func() {
		it.localSysStatus.Updated = tn
	}()

	if it.localSysStatus.Updated == 0 {
		it.localSysStatus.Caps = map[string]*kvapi.SysCapacity{
			"cpu": {
				Use: int64(runtime.NumCPU()),
			},
		}
		it.localSysStatus.Addr = it.cfg.Server.Bind
		it.localSysStatus.Version = version
		it.localSysStatus.Uptime = tn
	}

	if !it.cfg.Server.MetricsEnable {
		return nil
	}

	{
		// CPU
		if cio, _ := ps_cpu.Percent(0, false); len(cio) > 0 {
			metricGauge.Set(metricSystem+"CPUPercent", "Host", float64(cio[0]))
		}

		// RAM
		vm, _ := ps_mem.VirtualMemory()
		it.localSysStatus.Caps["mem"] = &kvapi.SysCapacity{
			Use: int64(vm.Used),
			Max: int64(vm.Total),
		}

		metricGauge.Set(metricSystem+"Memory", "HostUsed", float64(vm.Used))
		metricGauge.Set(metricSystem+"Memory", "HostCached", float64(vm.Cached))
		metricGauge.Set(metricSystem+"Memory", "HostTotal", float64(vm.Total))
		metricGauge.Set(metricSystem+"MemoryPercent", "Host", float64(vm.UsedPercent))
	}

	{
		// Network
		if nio, _ := ps_net.IOCounters(false); len(nio) > 0 {
			metricCounter.Set(metricSystem+"IO", "NetRecv", float64(nio[0].PacketsRecv))
			metricCounter.Set(metricSystem+"IOBytes", "NetRecv", float64(nio[0].BytesRecv))
			metricCounter.Set(metricSystem+"IO", "NetSent", float64(nio[0].PacketsSent))
			metricCounter.Set(metricSystem+"IOBytes", "NetSent", float64(nio[0].BytesSent))
		}
	}

	// Storage
	{
		if len(diskDevSet) == 0 {
			diskDevSet, _ = ps_disk.Partitions(false)
		}
		diskFilter := func(pls []ps_disk.PartitionStat, path string) string {
			path = filepath.Clean(path)
			for {
				for _, v := range pls {
					if path == v.Mountpoint {
						if strings.HasPrefix(v.Device, "/dev/") {
							return v.Device[5:]
						}
						return ""
					}
				}
				if i := strings.LastIndex(path, "/"); i > 0 {
					path = path[:i]
				} else if len(path) > 1 && path[0] == '/' {
					path = "/"
				} else {
					break
				}
			}
			return ""
		}
		if devName := diskFilter(diskDevSet, it.cfg.Storage.DataDirectory); devName != "" {
			if diom, err := ps_disk.IOCounters(devName); err == nil {
				if dio, ok := diom[devName]; ok {
					metricCounter.Set(metricSystem+"IO", "DiskRead", float64(dio.ReadCount))
					metricCounter.Set(metricSystem+"IOBytes", "DiskRead", float64(dio.ReadBytes))

					metricCounter.Set(metricSystem+"IO", "DiskWrite", float64(dio.WriteCount))
					metricCounter.Set(metricSystem+"IOBytes", "DiskWrite", float64(dio.WriteBytes))

					if prev, ok := diskStatusCaches[it.cfg.Storage.DataDirectory]; ok {
						if dio.ReadCount > prev.ReadCount {
							metricLatency.Add(metricSystem+"IO", "DiskRead",
								(float64(dio.ReadTime-prev.ReadTime)/1e3)/float64(dio.ReadCount-prev.ReadCount))
						}
						if dio.WriteCount > prev.WriteCount {
							metricLatency.Add(metricSystem+"IO", "DiskWrite",
								(float64(dio.WriteTime-prev.WriteTime)/1e3)/float64(dio.WriteCount-prev.WriteCount))
						}
					}
					diskStatusCaches[it.cfg.Storage.DataDirectory] = dio
				}
			}
		}
	}

	// Process
	{
		if p, err := ps_proc.NewProcess(int32(os.Getpid())); err == nil {
			if s, err := p.IOCounters(); err == nil {
				metricCounter.Set(metricSystem+"IO", "ProcRead", float64(s.ReadCount))
				metricCounter.Set(metricSystem+"IOBytes", "ProcRead", float64(s.ReadBytes))

				metricCounter.Set(metricSystem+"IO", "ProcWrite", float64(s.WriteCount))
				metricCounter.Set(metricSystem+"IOBytes", "ProcWrite", float64(s.WriteBytes))
			}
			if pp, err := p.CPUPercent(); err == nil {
				metricGauge.Set(metricSystem+"CPUPercent", "Proc", pp)
			}
			if pm, err := p.MemoryInfo(); err == nil {
				metricGauge.Set(metricSystem+"Memory", "ProcUsed", float64(pm.RSS))
			}
			if pmp, err := p.MemoryPercent(); err == nil {
				metricGauge.Set(metricSystem+"MemoryPercent", "Proc", float64(pmp))
			}
		}
	}

	// Disk
	if it.localSysStatus.Updated == 0 || mrand.Intn(10) == 0 {
		if st, err := ps_disk.Usage(it.cfg.Storage.DataDirectory); err == nil {
			it.localSysStatus.Caps["disk"] = &kvapi.SysCapacity{
				Use: int64(st.Used),
				Max: int64(st.Total),
			}
			metricGauge.Set(metricSystem+"Disk", "HostUsed", float64(st.Used))
			metricGauge.Set(metricSystem+"Disk", "HostTotal", float64(st.Total))
			if b, err := exec.Command("du", "-s", it.cfg.Storage.DataDirectory).Output(); err == nil {
				if ar := strings.Split(strings.TrimSpace(string(b)), "\t"); len(ar) == 2 {
					if i, err := strconv.Atoi(ar[0]); err == nil && i > 0 {
						metricGauge.Set(metricSystem+"Disk", "ProcUsed", float64(i*1024))
					}
				}
			}
		}
	}

	return nil
}
