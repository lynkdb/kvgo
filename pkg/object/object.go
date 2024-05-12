// Copyright 2015 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
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

package object

import (
	"io"
	"path/filepath"
	"strings"

	"github.com/lynkdb/kvgo/v2/internal/utils"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

const (
	kObjectBlockAttr_Version1   uint64 = 1 << 1
	kObjectBlockAttr_BlockSize2 uint64 = 1 << 2
	kObjectBlockAttr_BlockSize4 uint64 = 1 << 4

	kObjectBlockSize2 int64 = 2 * 1024 * 1024
	kObjectBlockSize4 int64 = 4 * 1024 * 1024
)

type ObjectClient interface {
	Put(path, localFilepath string) *kvapi.ResultSet
	Open(path string) (io.ReadSeeker, error)
}

type ObjectBlock struct {
	Num       uint32 `json:"num,omitempty"`
	Path      string `json:"path,omitempty"`
	Size      int64  `json:"size,omitempty"`
	Attrs     uint64 `json:"attrs,omitempty"`
	Data      []byte `json:"data,omitempty"`
	CommitKey string `json:"commit_key,omitempty"`
}

func pathClean(path string) string {

	s := strings.Trim(filepath.Clean("/"+path), ".")

	if len(s) > 1 && path[len(path)-1] == '/' {
		s += "/"
	}

	return s
}

func objPathKeyEncode(path string, n uint32) []byte {
	return []byte(path + ":" + utils.Uint32ToHexString(n))
}

func NewObjectBlock(
	path string,
	size int64,
	num uint32,
	data []byte,
) ObjectBlock {
	return ObjectBlock{
		Path:  pathClean(path),
		Size:  size,
		Num:   num,
		Attrs: kObjectBlockAttr_Version1 | kObjectBlockAttr_BlockSize2,
		Data:  data,
	}
}

func (it *ObjectBlock) AttrAllow(v uint64) bool {
	return attrAllow(it.Attrs, v)
}

func (it *ObjectBlock) BlockSize() int64 {
	if it.AttrAllow(kObjectBlockAttr_BlockSize2) {
		return kObjectBlockSize2
	} else if it.AttrAllow(kObjectBlockAttr_BlockSize4) {
		return kObjectBlockSize4
	}
	return 0
}

func (it *ObjectBlock) Valid() bool {

	if len(it.Path) < 1 || int(it.Size) != len(it.Data) {
		return false
	}

	blockSize := it.BlockSize()
	if blockSize == 0 {
		return false
	}

	numMax := uint32(it.Size / blockSize)
	if (it.Size % blockSize) == 0 {
		numMax -= 1
	}
	if it.Num > numMax {
		return false
	}
	if it.Num < numMax {
		if int64(len(it.Data)) != blockSize {
			return false
		}
	} else {
		if int64(len(it.Data)) != (it.Size % blockSize) {
			return false
		}
	}
	return true
}

func attrAllow(base, comp uint64) bool {
	return (comp & base) == comp
}

func attrAppend(base, comp uint64) uint64 {
	return base | comp
}

func attrRemove(base, comp uint64) uint64 {
	return (base | comp) - (comp)
}
