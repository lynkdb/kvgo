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

package object

import (
	"errors"
	"io"
	"os"

	"github.com/hooto/hlog4g/hlog"

	// kvclient "github.com/lynkdb/kvgo/v2/pkg/client"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func NewObjectClient(c kvapi.Client) (ObjectClient, error) {
	return &objectClient{
		kvclient: c,
	}, nil
}

type objectClient struct {
	kvclient kvapi.Client
}

func (cn *objectClient) Put(dstPath, localFilepath string) *kvapi.ResultSet {

	fp, err := os.Open(localFilepath)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}
	defer fp.Close()

	st, err := fp.Stat()
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}

	var (
		block0    = NewObjectBlock(dstPath, st.Size(), 0, nil)
		blockSize = block0.BlockSize()
	)

	if blockSize == 0 {
		return newResultSetWithClientError("protocol error")
	}

	if rs := cn.kvclient.Read(kvapi.NewReadRequest(objPathKeyEncode(block0.Path, 0))); rs.OK() {

		var prev ObjectBlock

		if err := rs.JsonDecode(&prev); err != nil {
			return newResultSetWithClientError(err.Error())
		}

		if prev.Size != st.Size() {
			return newResultSetWithClientError("protocol error")
		}

		block0.Attrs = prev.Attrs
	}

	num := uint32(block0.Size / blockSize)
	if num > 0 && (block0.Size%blockSize) == 0 {
		num -= 1
	}

	for n := uint32(0); n <= num; n++ {

		bsize := int(blockSize)
		if n == num {
			bsize = int(block0.Size % blockSize)
		}

		bs := make([]byte, bsize)
		if rn, err := fp.ReadAt(bs, int64(n)*blockSize); err != nil {
			return newResultSetWithClientError(err.Error())
		} else if rn != bsize {
			return newResultSetWithClientError("io error")
		} else {

			mpBlock := ObjectBlock{
				Path:  block0.Path,
				Size:  block0.Size,
				Attrs: block0.Attrs,
				Num:   n,
				Data:  bs,
			}

			mpKey := objPathKeyEncode(block0.Path, n)

			req := kvapi.NewWriteRequest(mpKey, mpBlock)

			if rs := cn.kvclient.Write(req); !rs.OK() {
				return rs
			}

			if n == num {
				hlog.Printf("info", "kvgo/fo size %d, block %d, path %s",
					block0.Size, n, mpKey)
			}
		}
	}

	return newResultSetOK()
}

func (cn *objectClient) Open(path string) (io.ReadSeeker, error) {

	path = pathClean(path)

	var (
		req = kvapi.NewReadRequest(objPathKeyEncode(path, 0))
	)

	rs := cn.kvclient.Read(req)
	if !rs.OK() {
		return nil, rs.Error()
	}

	var block0 ObjectBlock
	if err := rs.JsonDecode(&block0); err != nil {
		return nil, errors.New("ERR decode meta : " + err.Error())
	}

	return &ReadSeeker{
		kvclient: cn.kvclient,
		block0:   block0,
		path:     path,
		offset:   0,
	}, nil
}

type ReadSeeker struct {
	kvclient kvapi.Client
	block0   ObjectBlock
	blockx   *ObjectBlock
	path     string
	offset   int64
}

func (fo *ReadSeeker) Seek(offset int64, whence int) (int64, error) {

	abs := int64(0)

	switch whence {
	case 0:
		abs = offset

	case 1:
		abs = fo.offset + offset

	case 2:
		abs = offset + int64(fo.block0.Size)

	default:
		return 0, errors.New("invalid seek whence")
	}

	if abs < 0 {
		return 0, errors.New("out range of size")
	}
	fo.offset = abs

	return fo.offset, nil
}

func (fo *ReadSeeker) Read(b []byte) (n int, err error) {

	if len(b) == 0 {
		return 0, nil
	}

	blockSize := fo.block0.BlockSize()
	if blockSize == 0 {
		return 0, errors.New("protocol error")
	}

	blockNumMax := uint32(fo.block0.Size / blockSize)
	if (fo.block0.Size % blockSize) > 0 {
		blockNumMax += 1
	}

	var (
		nDone = 0
		nLen  = len(b)
	)

	for {

		if fo.offset >= int64(fo.block0.Size) {
			return nDone, io.EOF
		}

		var (
			blockNum    = uint32(fo.offset / blockSize)
			blockOffset = int(fo.offset % blockSize)
		)

		if blockNum > blockNumMax {
			return nDone, io.EOF
		}

		if blockNum == 0 {
			fo.blockx = &fo.block0
		}

		if fo.blockx == nil || fo.blockx.Num != blockNum {

			rs := fo.kvclient.Read(kvapi.NewReadRequest(objPathKeyEncode(fo.path, blockNum)))
			if !rs.OK() {
				return 0, errors.New("io error : " + rs.ErrorMessage())
			}

			var foBlock ObjectBlock
			if err := rs.JsonDecode(&foBlock); err != nil {
				return 0, errors.New("io error : " + err.Error())
			}

			if len(foBlock.Data) < 1 {
				return 0, errors.New("io error : invalid size")
			}

			fo.blockx = &foBlock
		}

		blockOffsetN := len(fo.blockx.Data) - blockOffset
		if blockOffsetN < 1 {
			return 0, errors.New("io error : offset")
		}
		if blockOffsetN > nLen {
			blockOffsetN = nLen
		}

		copy(b[nDone:], fo.blockx.Data[blockOffset:(blockOffset+blockOffsetN)])

		fo.offset += int64(blockOffsetN)
		nDone += blockOffsetN
		nLen -= blockOffsetN

		if nLen < 1 {
			break
		}
	}

	return nDone, nil
}
