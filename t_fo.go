// Copyright 2018 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
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

package kvgo

import (
	"errors"
	"io"
	"os"

	"github.com/lynkdb/iomix/sko"
	"github.com/lynkdb/iomix/skv"
)

func foFilePathBlock(path string, n uint32) []byte {
	return []byte(path + ":" + sko.Uint32ToHexString(n))
}

func (cn *SkoConn) FoFilePut(src_path, dst_path string) skv.Result {

	fp, err := os.Open(src_path)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}

	st, err := fp.Stat()
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}

	if st.Size() < 1 {
		return newResult(skv.ResultBadArgument, errors.New("invalid file size"))
	}

	mpInit := skv.NewFileObjectEntryBlock(dst_path, uint64(st.Size()),
		0, nil, "")
	block_size := uint64(0)

	if ors := cn.NewReader(foFilePathBlock(mpInit.Path, 0)).Query(); ors.OK() {

		var block0 skv.FileObjectEntryBlock

		if err := ors.Decode(&block0); err != nil {
			return newResult(skv.ResultBadArgument, err)
		}

		if block0.Size != uint64(st.Size()) {
			return newResult(skv.ResultBadArgument, errors.New("protocol error"))
		}

		mpInit.Attrs = block0.Attrs
	}

	if mpInit.AttrAllow(skv.FileObjectEntryAttrBlockSize4) {
		block_size = skv.FileObjectBlockSize4
	}

	if block_size == 0 {
		return newResult(skv.ResultBadArgument, errors.New("protocol error"))
	}

	num := uint32(mpInit.Size / block_size)
	if num > 0 && (mpInit.Size%block_size) == 0 {
		num -= 1
	}

	for n := uint32(0); n <= num; n++ {

		bsize := int(block_size)
		if n == num {
			bsize = int(mpInit.Size % block_size)
		}

		bs := make([]byte, bsize)
		if rn, err := fp.ReadAt(bs, int64(n)*int64(block_size)); err != nil {
			return newResult(skv.ResultBadArgument, err)
		} else if rn != bsize {
			return newResult(skv.ResultBadArgument, errors.New("io error"))
		} else {

			mpBlock := skv.FileObjectEntryBlock{
				Path:  mpInit.Path,
				Size:  mpInit.Size,
				Attrs: mpInit.Attrs,
				Num:   n,
				Data:  bs,
			}

			if rs := cn.NewWriter(foFilePathBlock(mpInit.Path, n), mpBlock).
				Commit(); !rs.OK() {
				return newResult(skv.ResultServerError, rs.Error())
			}

		}
	}

	return newResult(skv.ResultOK, nil)
}

type FoReadSeeker struct {
	conn   *SkoConn
	block0 skv.FileObjectEntryBlock
	blockx *skv.FileObjectEntryBlock
	path   string
	offset int64
}

func (fo *FoReadSeeker) Seek(offset int64, whence int) (int64, error) {

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

func (fo *FoReadSeeker) Read(b []byte) (n int, err error) {

	if len(b) == 0 {
		return 0, nil
	}

	block_size := int64(0)
	if fo.block0.AttrAllow(skv.FileObjectEntryAttrBlockSize4) {
		block_size = int64(skv.FileObjectBlockSize4)
	}
	if block_size == 0 {
		return 0, errors.New("protocol error")
	}

	blk_num_max := uint32(fo.block0.Size / uint64(block_size))
	if (fo.block0.Size % uint64(block_size)) > 0 {
		blk_num_max += 1
	}

	var (
		n_done = 0
		n_len  = len(b)
	)

	for {

		if fo.offset >= int64(fo.block0.Size) {
			return n_done, io.EOF
		}

		var (
			blk_num = uint32(fo.offset / block_size)
			blk_off = int(fo.offset % block_size)
		)

		if blk_num > blk_num_max {
			return n_done, io.EOF
		}

		if blk_num == 0 {
			fo.blockx = &fo.block0
		}

		if fo.blockx == nil || fo.blockx.Num != blk_num {

			rs := fo.conn.NewReader(foFilePathBlock(fo.path, blk_num)).Query()
			if !rs.OK() {

				return 0, errors.New("io error")
			}

			var foBlock skv.FileObjectEntryBlock
			if err := rs.Decode(&foBlock); err != nil {
				return 0, errors.New("io error")
			}

			if len(foBlock.Data) < 1 {
				return 0, errors.New("io error")
			}

			fo.blockx = &foBlock
		}

		blk_off_n := len(fo.blockx.Data) - blk_off
		if blk_off_n < 1 {
			return 0, errors.New("offset error")
		}
		if blk_off_n > n_len {
			blk_off_n = n_len
		}

		copy(b[n_done:], fo.blockx.Data[blk_off:(blk_off+blk_off_n)])

		fo.offset += int64(blk_off_n)
		n_done += blk_off_n
		n_len -= blk_off_n

		if n_len < 1 {
			break
		}
	}

	return n_done, nil
}

func (cn *SkoConn) FoFileOpen(path string) (io.ReadSeeker, error) {

	rs := cn.NewReader(foFilePathBlock(path, 0)).Query()
	if !rs.OK() {
		return nil, rs.Error()
	}

	var block0 skv.FileObjectEntryBlock
	if err := rs.Decode(&block0); err != nil {
		return nil, errors.New("ER decode meta : " + err.Error())
	}

	return &FoReadSeeker{
		conn:   cn,
		block0: block0,
		path:   path,
		offset: 0,
	}, nil
}
