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
	"fmt"
	"hash/crc32"
	"time"
)

func AttrAllow(base, comp uint64) bool {
	return (comp & base) == comp
}

func AttrAppend(base, comp uint64) uint64 {
	return base | comp
}

func AttrRemove(base, comp uint64) uint64 {
	return (base | comp) - (comp)
}

func bytesCrc32Checksum(bs []byte) uint64 {
	sumCheck := crc32.ChecksumIEEE(bs)
	if sumCheck == 0 {
		sumCheck = 1
	}
	return uint64(sumCheck)
}

func timems() int64 {
	return time.Now().UnixNano() / 1e6
}

func BytesHumanDisplay(v int64) string {

	if v <= 0 {
		return "0"
	}

	type hrd struct {
		name string
		base int64
	}

	var bytesHumanReadableDicts = []hrd{
		{"PB", PiB},
		{"TB", TiB},
		{"GB", GiB},
		{"MB", MiB},
		{"KB", KiB},
	}

	for _, r := range bytesHumanReadableDicts {
		if v >= r.base {
			return fmt.Sprintf("%.2f "+r.name, float64(v)/float64(r.base))
		}
	}

	return fmt.Sprintf("%d B", v)
}
