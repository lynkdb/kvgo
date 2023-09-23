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

	"github.com/cespare/xxhash"

	hauth "github.com/hooto/hauth/go/hauth/v1"
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
	sysTableId      = "1"
	sysTableName    = "system"
	sysTableStoreId = "01"
)

const (
	writeProposalTTL int64 = 5000

	logRetentionMilliseconds int64 = 86400 * 2 * 1e3
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

func nsSysTable(id string) []byte {
	return []byte("sys:table:" + id)
}

func nsSysTableMap(id string) []byte {
	return []byte("sys:tablemap:" + id)
}

var (
	authPermSysAll     = "sys/all"
	authPermTableList  = "table/list"
	authPermTableRead  = "table/read"
	authPermTableWrite = "table/write"
	AuthScopeTable     = "kvgo/table"
	defaultScopes      = []string{
		AuthScopeTable,
	}
	defaultRoles = []*hauth.Role{
		{
			Name:  "sa",
			Title: "System Administrator",
			Permissions: []string{
				authPermSysAll,
				authPermTableList,
				authPermTableRead,
				authPermTableWrite,
			},
		},
		{
			Name:  "client",
			Title: "General Client",
			Permissions: []string{
				authPermTableList,
				authPermTableRead,
				authPermTableWrite,
			},
		},
	}
)
