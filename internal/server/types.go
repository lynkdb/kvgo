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
	"encoding/base64"
	"encoding/binary"
	"sort"
)

type logPullReplicaState struct {
	ShardId      uint64 `json:"shard_id"`
	ReplicaId    uint64 `json:"replica_id"`
	SrcReplicaId uint64 `json:"src_replica_id"`
	SrcLogOffset uint64 `json:"src_log_offset"`
	SrcKeyOffset []byte `json:"src_key_offset,omitempty"`
	FullScan     bool   `json:"full_scan,omitempty"`
}

type dbReplicaLogState struct {
	RetentionOffset uint64 `json:"retention_offset"`
	Offset          uint64 `json:"offset"`
	Cutset          uint64 `json:"cutset"`
}

type logRangeTokenItem struct {
	store  uint64
	offset uint64
}

type logRangeToken struct {
	version uint64
	data1   []uint64
	index1  map[uint64]uint64

	nextIndex []*logRangeTokenItem
}

type jobTransferInOffset struct {
	UniId    string `json:"uni_id"`
	LogToken string `json:"log_token"`

	FullScan  bool   `json:"full_scan"`
	KeyOffset []byte `json:"key_offset,omitempty"`

	Updated int64 `json:"updated"`

	Stats struct {
		DeltaLogRead   int64 `json:"delta_log_read"`
		DeltaDataRead  int64 `json:"delta_data_read"`
		DeltaDataFlush int64 `json:"delta_data_flush"`

		FullPull      int64 `json:"full_pull"`
		FullMetaRead  int64 `json:"full_meta_read"`
		FullDataRead  int64 `json:"full_data_read"`
		FullDataFlush int64 `json:"full_data_flush"`

		LocalMetaRead int64 `json:"local_meta_read"`
		LocalMetaSkip int64 `json:"local_meta_skip"`
		LocalDelete   int64 `json:"local_delete"`

		MergeDeny int64 `json:"merge_deny"`
		MergeSkip int64 `json:"merge_skip"`
	} `json:"stats"`
}

func newLogRangeToken(s string) *logRangeToken {
	tk := &logRangeToken{
		version: 1,
		index1:  map[uint64]uint64{},
	}
	if len(s) > 0 {
		b, err := base64.StdEncoding.DecodeString(s)
		if err == nil {
			for i := 0; i < len(b); {
				v, n := binary.Uvarint(b[i:])
				if i == 0 {
					tk.version = v
				} else {
					tk.data1 = append(tk.data1, v)
				}
				i += n
			}
			if len(tk.data1)%2 == 0 {
				for i := 1; i < len(tk.data1); i += 2 {
					tk.index1[tk.data1[i-1]] = tk.data1[i]
				}
			}
		}
	}
	return tk
}

func (it *logRangeToken) indexApply(storeOffsets map[uint64]uint64) {
	it.index1 = map[uint64]uint64{}
	it.nextIndex = []*logRangeTokenItem{}
	for id, v := range storeOffsets {
		it.index1[id] = v
		it.nextIndex = append(it.nextIndex, &logRangeTokenItem{
			store:  id,
			offset: v,
		})
	}
}

func (it *logRangeToken) nextApply(storeIds map[uint64]uint64) {
	it.nextIndex = []*logRangeTokenItem{}
	for id, _ := range storeIds {
		if v, ok := it.index1[id]; ok {
			it.nextIndex = append(it.nextIndex, &logRangeTokenItem{
				store:  id,
				offset: v,
			})
		} else {
			it.nextIndex = append(it.nextIndex, &logRangeTokenItem{
				store:  id,
				offset: 0,
			})
		}
	}
}

func (it *logRangeToken) encode() string {
	if len(it.nextIndex) > 0 {
		it.data1 = make([]uint64, 0, len(it.nextIndex)*2)
		sort.Slice(it.nextIndex, func(i, j int) bool {
			return it.nextIndex[i].store < it.nextIndex[j].store
		})
		for _, v := range it.nextIndex {
			it.data1 = append(it.data1, []uint64{v.store, v.offset}...)
		}
	}
	var buf = binary.AppendUvarint([]byte{}, it.version)
	for _, v := range it.data1 {
		buf = binary.AppendUvarint(buf, v)
	}
	return base64.StdEncoding.EncodeToString(buf)
}
