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
	"sync"

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

type sysStatus struct {
	mu      sync.RWMutex
	volumes map[string]*kvapi.SysVolumeStatus
}

func (it *sysStatus) syncVolumeStatus(id string, used, free int64) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.volumes == nil {
		it.volumes = map[string]*kvapi.SysVolumeStatus{}
	}
	vol, ok := it.volumes[id]
	if !ok {
		vol = &kvapi.SysVolumeStatus{
			Id: id,
		}
		it.volumes[id] = vol
	}
	vol.CapacityUsed = uint64(used)
	vol.CapacityFree = uint64(free)
	vol.Updated = uint64(timesec())
}
