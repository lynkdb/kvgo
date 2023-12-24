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
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash"

	hauth "github.com/hooto/hauth/go/hauth/v1"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

var (
	testLocalMode = false
)

const (
	nsKeySys  uint8 = 16
	nsKeyMeta uint8 = 17
	nsKeyData uint8 = 18
	nsKeyLog  uint8 = 19
	nsKeyTtl  uint8 = 20
)

const (
	grpcMsgByteMax = 12 << 20

	maxReplicaCap = 5
)

const (
	StandaloneMode = "v2x1"
)

var (
	keySysInstanceId = append([]byte{nsKeySys}, []byte("inst:id")...)
	keySysVerCutset  = append([]byte{nsKeySys}, []byte("ver:cutset")...)
	keySysLogState   = append([]byte{nsKeySys}, []byte("log:state")...)

	keySysLogPullCutset = append([]byte{nsKeySys}, []byte("log-pull:offset")...)
)

const (
	sysDatabaseId             = "00000001"
	sysDatabaseName           = "system"
	sysDatabaseStoreId uint64 = 1
)

const (
	kDatabaseInstanceMax = 256
)

const (
	writeProposalTTL int64 = 5000

	logRetentionMilliseconds int64 = 86400 * 2 * 1e3
)

var (
	shardSplit_CapacitySize_Def int64 = 8 << 30
	shardSplit_CapacitySize_Min int64 = 512 << 20
	shardSplit_CapacitySize_Max int64 = 128 << 30
)

const (
	shardSplit_CapacityThreshold = float64(0.5)
	shardSplit_CapacityBiasMax   = float64(0.1)
	shardSplit_CapacityBiasMin   = float64(0.01)
)

var (
	replicaRebalance_StoreCapacityThreshold = float64(0.25)

	replicaRemoveFromDatabaseMapDelaySeconds int64 = 60
)

const (
	kReplicaSetup_In  uint64 = 1 << 0
	kReplicaSetup_Out uint64 = 1 << 1

	kReplicaSetup_MoveIn  uint64 = 1 << 2
	kReplicaSetup_MoveOut uint64 = 1 << 3

	kReplicaSetup_Remove uint64 = 1 << 7
)

const (
	kReplicaStatus_In  uint64 = 1 << 8
	kReplicaStatus_Out uint64 = 1 << 9

	kReplicaStatus_Ready  uint64 = 1 << 10
	kReplicaStatus_Remove uint64 = 1 << 11
)

const (
	kJob_ShardResetIntervalSeconds int64 = 60
)

var (
	kReplicaSetupMap = map[uint64]string{
		kReplicaSetup_In:      "In",
		kReplicaSetup_Out:     "Out",
		kReplicaSetup_MoveIn:  "MoveIn",
		kReplicaSetup_MoveOut: "MoveOut",
		kReplicaSetup_Remove:  "Remove",
	}
	kReplicaStatusMap = map[uint64]string{
		kReplicaStatus_In:     "In",
		kReplicaStatus_Out:    "Out",
		kReplicaStatus_Ready:  "Ready",
		kReplicaStatus_Remove: "Remove",
	}
)

func replicaActionDisplay(v uint64) string {
	var arr []string
	for a, s := range kReplicaSetupMap {
		if kvapi.AttrAllow(v, a) {
			arr = append(arr, s)
		}
	}
	for a, s := range kReplicaStatusMap {
		if kvapi.AttrAllow(v, a) {
			arr = append(arr, s)
		}
	}
	if len(arr) == 0 {
		return "UnSpec"
	}
	sort.Slice(arr, func(i, j int) bool {
		return strings.Compare(arr[i], arr[j]) < 0
	})
	return strings.Join(arr, ",")
}

const (
	kShardSetup_In  uint64 = 1 << 0
	kShardSetup_Out uint64 = 1 << 1

	kShardSetup_SplitIn  uint64 = 1 << 4
	kShardSetup_SplitOut uint64 = 1 << 5

	kShardSetup_Rebalance uint64 = 1 << 6
)

const (
	kShardStatus_In uint64 = 1 << 8
)

var (
	kShardSetupMap = map[uint64]string{
		kShardSetup_In:        "In",
		kShardSetup_Out:       "Out",
		kShardSetup_SplitIn:   "SplitIn",
		kShardSetup_SplitOut:  "SplitOut",
		kShardSetup_Rebalance: "Rebalance",
	}
	kShardStatusMap = map[uint64]string{
		kShardStatus_In: "In",
	}
)

func shardActionDisplay(v uint64) string {
	var arr []string
	for a, s := range kShardSetupMap {
		if kvapi.AttrAllow(v, a) {
			arr = append(arr, s)
		}
	}
	for a, s := range kShardStatusMap {
		if kvapi.AttrAllow(v, a) {
			arr = append(arr, s)
		}
	}
	if len(arr) == 0 {
		return "UnSpec"
	}
	return strings.Join(arr, ",")
}

const (
	dbReplicaStatusRefreshIntervalSecond int64 = 1200

	jobStoreStatusRefreshIntervalSecond int64 = 10 // TODO
)

func keyEncode(ns byte, key []byte) []byte {
	return append([]byte{ns}, key...)
}

func keyExpireEncode(replicaId uint64, expired int64, key []byte) []byte {
	k := append(append([]byte{nsKeyTtl}, uint64ToBytes(replicaId)...), uint64ToBytes(uint64(expired))...)
	if len(key) == 0 {
		return k
	}
	return append(k, uint64ToBytes(xxhash.Sum64(key))...)
}

func keyLogEncode(replicaId, logVersion uint64) []byte {
	return append(append([]byte{nsKeyLog}, uint64ToBytes(replicaId)...), uint64ToBytes(logVersion)...)
}

func keySysIncrCutset(ns string) []byte {
	return append([]byte{nsKeySys}, []byte("incr:cutset:"+ns)...)
}

func keySysLogPullOffset(dstReplicaId, srcReplicaId uint64) []byte {
	return append([]byte{nsKeySys}, []byte(fmt.Sprintf("log-pull:offset:%020d:%020d", dstReplicaId, srcReplicaId))...)
}

func nsSysStore(uniId string) []byte {
	return []byte("sys/store/" + uniId)
}

func nsSysDatabase(id string) []byte {
	return []byte("sys/dbspec/" + id)
}

func nsSysDatabaseMap(id string) []byte {
	return []byte("sys/dbmap/" + id)
}

func nsSysAuditLog(b bool) []byte {
	if !b {
		return []byte("sys/auditlog/")
	}
	return []byte("sys/auditlog/" + uint64ToHexString(uint64(timens())))
}

var (
	authPermSysAll        = "sys/all"
	authPermDatabaseList  = "db/list"
	authPermDatabaseRead  = "db/read"
	authPermDatabaseWrite = "db/write"
	AuthScopeDatabase     = "kvgo/db"
	defaultScopes         = []string{
		AuthScopeDatabase,
	}
	defaultRoles = []*hauth.Role{
		{
			Name:  "sa",
			Title: "System Administrator",
			Permissions: []string{
				authPermSysAll,
				authPermDatabaseList,
				authPermDatabaseRead,
				authPermDatabaseWrite,
			},
		},
		{
			Name:  "client",
			Title: "General Client",
			Permissions: []string{
				authPermDatabaseList,
				authPermDatabaseRead,
				authPermDatabaseWrite,
			},
		},
	}
)