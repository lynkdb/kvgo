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

package kvapi

import (
	"regexp"
)

var (
	DatabaseNameRX = regexp.MustCompile("^[a-z]{1}[a-z0-9\\-\\_\\/\\:\\.]{3,63}$")

	IncrNamespaceRX = regexp.MustCompile("^[a-z]{1}[a-z0-9_]{3,31}$")
)

// Status Code defines
const (
	Status_OK              uint32 = 2000
	Status_InvalidArgument uint32 = 4000
	Status_NotFound        uint32 = 4040
	Status_RequestTimeout  uint32 = 4080
	Status_Conflict        uint32 = 4090
	Status_AuthDeny        uint32 = 4010
	Status_VersionConflict uint32 = 4091
	Status_ServerError     uint32 = 5000
)

const (
	Read_Attrs_MetaOnly uint64 = 1 << 2
	Read_Attrs_RevRange uint64 = 1 << 3

	Write_Attrs_Delete     uint64 = 1 << 6
	Write_Attrs_RetainMeta uint64 = 1 << 7

	Write_Attrs_IgnoreMeta uint64 = 1 << 8
	Write_Attrs_IgnoreData uint64 = 1 << 9

	Write_Attrs_Sync uint64 = 1 << 10
)

const (
	Service_Range_MaxLimit = 10000
	Service_Range_MaxSize  = 4 << 20
	Service_Range_DefSize  = 4 << 20
)

const (
	// 1:version|1:meta-size|meta-bytes|data-bytes
	keyValueMetaVersion1 uint8 = 2

	// 1:version|2:meta-size|meta-bytes|data-bytes
	keyValueMetaVersion2 uint8 = 3

	keyValue_Meta_MaxSize = 4096
)

const (
	keyValue_Meta_Extra_MaxLen = 512
)

const (
	keyValue_MinKeyLen = 1
	keyValue_MaxKeyLen = 256
)

const (
	KiB = int64(1024)
	MiB = KiB * 1024
	GiB = MiB * 1024
	TiB = GiB * 1024
	PiB = TiB * 1024
)
