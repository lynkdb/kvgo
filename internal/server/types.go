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

type logPullReplicaState struct {
	ShardId      uint64 `json:"shard_id"`
	ReplicaId    uint64 `json:"replica_id"`
	SrcReplicaId uint64 `json:"src_replica_id"`
	SrcLogOffset uint64 `json:"src_log_offset"`
	SrcKeyOffset []byte `json:"src_key_offset,omitempty"`
	FullScan     bool   `json:"full_scan,omitempty"`
}

type dbReplicaLogState struct {
	RetentionOffset uint64 `json:retention_offset"`
	Offset          uint64 `json:"offset"`
	Cutset          uint64 `json:"cutset"`
}

type dbReplicaIncrState struct {
	Namespace string `json:"namespace"`
	Offset    uint64 `json:"offset"`
	Cutset    uint64 `json:"cutset"`
}
