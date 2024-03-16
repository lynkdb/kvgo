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

package tests

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func ClientAPI(c kvapi.Client, t *testing.T) {

	// Write
	{
		if rs := c.Write(kvapi.NewWriteRequest([]byte("0001"), []byte("1"))); !rs.OK() {
			t.Fatalf("Write ER!, Err %s", rs.ErrorMessage())
		} else {
			t.Logf("Write OK, Log %d", rs.Meta().Version)
		}
	}

	// Read-Key
	{
		if rs := c.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.OK() {
			t.Fatalf("Read-Key ER! %s", rs.ErrorMessage())
		} else {
			if rs.Item() != nil && rs.Item().Uint64Value() != 1 {
				t.Fatal("Read-Key ER! Compare")
			} else if rs.MaxVersion < 100 {
				t.Fatal("Read-Key ER! MaxVersion")
			} else if bytes.Compare(rs.Item().Key, []byte("0001")) != 0 {
				t.Fatal("Read-Key ER! Key")
			} else {
				t.Logf("Read-Key OK, MaxVersion %d", rs.MaxVersion)
			}
		}
	}

	// Delete
	{
		if rs := c.Delete(kvapi.NewDeleteRequest([]byte("0001"))); !rs.OK() {
			t.Fatal("Delete-Key ER!")
		} else {
			t.Logf("Delete-Key Version %d", rs.Meta().Version)
		}
		if rs := c.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.NotFound() {
			t.Fatal("Delete-ER!")
		} else {
			t.Logf("Delete-Key OK")
		}
	}

	// Log
	{
		key := []byte("log-id-test")
		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("123"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		}
		logID := uint64(0)
		if rs := c.Read(kvapi.NewReadRequest(key)); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else if rs.Meta().Version < 1 {
			t.Fatal("Write LogId ER!")
		} else {
			logID = rs.Meta().Version
		}

		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("123"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		}
		if rs := c.Read(kvapi.NewReadRequest(key)); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else if rs.Meta().Version != logID {
			t.Fatalf("Write LogId ER! %d - %d", rs.Meta().Version, logID)
		}

		if rs := c.Write(kvapi.NewWriteRequest(key, []byte("321"))); !rs.OK() {
			t.Fatal("Write LogId ER!")
		}
		if rs := c.Read(kvapi.NewReadRequest(key)); !rs.OK() {
			t.Fatal("Write LogId ER!")
		} else if rs.Meta().Version <= logID {
			t.Fatal("Write LogId ER!")
		} else {
			t.Logf("Write LogId OK from %d to %d", logID, rs.Meta().Version)
		}
	}

	//
	for _, n := range []int{1, 2, 3} {
		c.Write(kvapi.NewWriteRequest([]byte(fmt.Sprintf("%04d", n)), []byte(fmt.Sprintf("%v", n))))
	}

	// Range
	{
		req := &kvapi.RangeRequest{
			LowerKey: []byte("0001"),
			UpperKey: []byte("0009"),
			Limit:    10,
		}
		if rs := c.Range(req); !rs.OK() {
			t.Fatal("Range ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range ER! %v", rs.Items)
				t.Fatalf("Range ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if len(item.Value) == 0 || item.Int64Value() != int64(i+2) {
					t.Fatal("Range ER!")
				}
			}

			t.Logf("Range OK")
		}

		req.Attrs = kvapi.Read_Attrs_MetaOnly
		if rs := c.Range(req); !rs.OK() {
			t.Fatal("Range ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range ER! %d", len(rs.Items))
			}

			for _, item := range rs.Items {
				if item.Value != nil || len(item.Key) == 0 {
					t.Fatal("Range ER!")
				}
			}

			t.Logf("Range OK")
		}
	}

	// Range+RevRange
	{
		req := &kvapi.RangeRequest{
			LowerKey: []byte("0000"),
			UpperKey: []byte("0003"),
			Limit:    10,
			Revert:   true,
		}
		if rs := c.Range(req); !rs.OK() {
			t.Fatal("Range+Rev ER!")
		} else {

			if len(rs.Items) != 2 {
				t.Fatalf("Range+Rev ER! %d", len(rs.Items))
			}

			for i, item := range rs.Items {
				if len(item.Value) == 0 || item.Int64Value() != int64(2-i) {
					t.Logf("Range+Rev ER! #%d %v", 2-i, item.Value)
				}
			}

			t.Logf("Range+Rev OK")
		}
	}

	// Write Expired
	{
		if rs := c.Write(kvapi.NewWriteRequest([]byte("0001"), []byte("ttl")).SetTTL(500)); !rs.OK() {
			t.Fatal("Write Expirted ER!")
		}
		if rs := c.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.OK() {
			t.Fatal("Write Expirted ER! Read")
		}
		if rs := c.Range(kvapi.NewRangeRequest([]byte("0000"), []byte("0001z"))); !rs.OK() {
			t.Fatal("Write Expirted ER! Range")
		}
		time.Sleep(1e9)
		if rs := c.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.NotFound() {
			t.Fatalf("Write Expirted ER! Read")
		} else {
			t.Logf("Write Expirted OK")
		}
		if rs := c.Range(kvapi.NewRangeRequest([]byte("0000"), []byte("0001z"))); !rs.NotFound() {
			t.Fatalf("Write Expirted ER! Range %v", rs)
		} else {
			t.Logf("Write Expirted OK")
		}
	}

	round := 1

	// Write IncrId
	{
		var (
			incrNS = fmt.Sprintf("nsdef%d", round)
			key    = []byte(fmt.Sprintf("incr-id-1-%d", round))
		)

		req := kvapi.NewWriteRequest(key, []byte("demo"))

		req.IncrNamespace = incrNS
		req.Meta.IncrId = 1000

		if rs := c.Write(req); !rs.OK() {
			t.Fatalf("Write IncrId ER! %s", rs.ErrorMessage())
		} else {
			t.Logf("Write IncrId OK, incr-id %d", rs.Meta().IncrId)
		}
		if rs := c.Read(kvapi.NewReadRequest(key)); !rs.OK() || rs.Items[0].Meta.IncrId != 1000 {
			t.Fatalf("Read IncrId ER! %s", rs.ErrorMessage())
		} else {
			t.Logf("Read IncrId OK, incr-id %d", rs.Meta().IncrId)
		}
		key = []byte(fmt.Sprintf("incr-id-2-%d", round))

		req = kvapi.NewWriteRequest(key, []byte("demo"))
		req.IncrNamespace = incrNS

		if rs := c.Write(req); !rs.OK() {
			t.Fatal("Write-2 IncrId ER!")
		} else {
			t.Logf("Write-2 IncrId OK, incr-id %d", rs.Meta().IncrId)
		}
		if rs := c.Read(kvapi.NewReadRequest(key)); !rs.OK() || rs.Items[0].Meta.IncrId <= 1000 {
			t.Fatal("Write-2 IncrId ER!")
		} else {
			t.Logf("Write-2 IncrId OK, incr-id %d", rs.Meta().IncrId)
		}

		// Prev IncrId Check
		req = kvapi.NewWriteRequest(key, []byte("demo"))
		req.IncrNamespace = incrNS
		req.Meta.IncrId = 2000
		req.PrevIncrId = 1000
		if rs := c.Write(req); rs.OK() {
			t.Fatal("Write PrevIncrId ER!")
		} else {
			t.Logf("Write PrevIncrId OK, meta %v", rs.Meta())
		}
		req.PrevIncrId = 1001
		if rs := c.Write(req); rs.OK() {
			t.Logf("Write PrevIncrId OK, incr-id %d", rs.Meta().IncrId)
		} else {
			t.Fatalf("Write PrevIncrId ER! %s", rs.StatusMessage)
		}
		if rs := c.Read(kvapi.NewReadRequest(key)); rs.OK() && rs.Meta().IncrId == req.Meta.IncrId {
			t.Log("Write PrevIncrId OK")
		} else {
			t.Fatalf("Write PrevIncrId Check ER! %d", rs.Meta().IncrId)
		}
	}

	// Prev Attr Check
	{
		var (
			key = []byte(fmt.Sprintf("attr-id-1-%d", 1))
			req = kvapi.NewWriteRequest(key, []byte("demo"))
		)

		req.Meta.Attrs |= (1 << 48)
		req.Meta.Attrs |= (1 << 49)

		if rs := c.Write(req); rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}

		if rs := c.Read(kvapi.NewReadRequest(key)); rs.OK() &&
			kvapi.AttrAllow(rs.Meta().Attrs, (1<<48)) &&
			kvapi.AttrAllow(rs.Meta().Attrs, (1<<49)) {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatalf("Write PrevAttrs Check ER!")
		}

		req = kvapi.NewWriteRequest(key, []byte("demo-1"))
		req.PrevAttrs |= (1 << 50)
		if rs := c.Write(req); !rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}
		req = kvapi.NewWriteRequest(key, []byte("demo-1"))
		req.PrevAttrs |= (1 << 48)
		if rs := c.Write(req); rs.OK() {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatal("Write PrevAttrs ER!")
		}

		if rs := c.Read(kvapi.NewReadRequest(key)); rs.OK() &&
			kvapi.AttrAllow(rs.Items[0].Meta.Attrs, (1<<48)) &&
			kvapi.AttrAllow(rs.Items[0].Meta.Attrs, (1<<49)) {
			t.Log("Write PrevAttrs OK")
		} else {
			t.Fatalf("Write PrevAttrs Check ER!")
		}
	}

	// Write Attrs Ignore Meta|Data
	{
		for _, va := range [][]uint64{
			{kvapi.Write_Attrs_IgnoreData, kvapi.Read_Attrs_MetaOnly, 0},
			{kvapi.Write_Attrs_IgnoreMeta, 0, kvapi.Read_Attrs_MetaOnly},
		} {

			key := []byte(fmt.Sprintf("data-off-%d-%d", round, va[0]))
			req := kvapi.NewWriteRequest(key, []byte("demo")).SetAttrs(va[0])
			if rs := c.Write(req); rs.OK() {
				t.Log("Write Attrs Ignore Meta|Data OK")
			} else {
				t.Fatal("Write Attrs Ignore Meta|Data ER!")
			}

			if rs := c.Read(kvapi.NewReadRequest(key).SetAttrs(va[1])); rs.OK() {
				t.Log("Write Attrs Ignore Meta|Data OK")
			} else {
				t.Fatal("Write Attrs Ignore Meta|Data ER!")
			}

			if rs := c.Read(kvapi.NewReadRequest(key).SetAttrs(va[2])); rs.NotFound() {
				t.Log("Write Attrs Ignore Meta|Data OK")
			} else {
				t.Fatal("Write Attrs Ignore Meta|Data ER!")
			}
		}
	}

	// Delete Data Only
	{
		var (
			key = []byte(fmt.Sprintf("delete-data-only-%d", round))
			req = kvapi.NewWriteRequest(key, []byte("demo"))
		)
		if rs := c.Write(req); rs.OK() {
			t.Log("Delete Attrs RetainMeta Step-1 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-1 ER!")
		}

		if rs := c.Delete(kvapi.NewDeleteRequest(key).SetRetainMeta(true)); rs.OK() {
			t.Log("Delete Attrs RetainMeta Step-2 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-2 ER!")
		}

		if rs := c.Read(kvapi.NewReadRequest(key)); rs.NotFound() {
			t.Log("Delete Attrs RetainMeta Step-3 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-3 ER!")
		}

		if rs := c.Read(kvapi.NewReadRequest(key).SetMetaOnly(true)); rs.OK() {
			t.Logf("Delete Attrs RetainMeta Step-4 OK")
		} else {
			t.Fatal("Delete Attrs RetainMeta Step-4 ER!")
		}
	}

	// Write Meta Extra
	{
		var (
			extra = append([]byte("extra-data"), bytes.Repeat([]byte{0x00}, 256)...)
			key   = []byte("meta-extra-data")
			value = []byte("demo")
			req   = kvapi.NewWriteRequest(key, value)
		)

		req.Meta.Extra = extra
		if rs := c.Write(req); rs.OK() {
			t.Log("Write Meta Extra OK")
		} else {
			t.Fatalf("Write Meta Extra ER! %s", rs.StatusMessage)
		}

		if rs := c.Read(kvapi.NewReadRequest(key)); rs.OK() &&
			rs.Item() != nil && bytes.Compare(rs.Item().Value, value) == 0 &&
			bytes.Compare(extra, rs.Meta().Extra) == 0 {
			t.Log("Write Meta Extra OK")
		} else {
			t.Fatal("Write Meta Extra ER!")
		}

		if rs := c.Read(kvapi.NewReadRequest(key).SetMetaOnly(true)); rs.OK() &&
			rs.Item() != nil &&
			len(rs.Item().Value) == 0 &&
			bytes.Compare(extra, rs.Item().Meta.Extra) == 0 {
			t.Log("Write Meta Extra OK")
		} else {
			t.Fatal("Write Meta Extra ER!")
		}
	}

	/**
	// Write Struct
	{
		obj := kvapi.Meta{
			Attrs: 100,
		}
		if rs := c.Write(kvapi.NewWriteRequest([]byte("0001"), obj); !rs.OK() {
			t.Fatal("Write ER!")
		}
		if rs := c.Read(kvapi.NewReadRequest([]byte("0001"))); !rs.OK() {
			t.Fatalf("Read-Key ER! status %d", rs.Status)
		} else {
			var item kvapi.Meta
			if err := rs.Item().JsonDecode(&item); err != nil {
				t.Fatalf("Read-Key Value().Decode() ER! %s", err.Error())
			}
			if item.Attrs != 100 {
				t.Fatal("Read-Key Value().Decode() ER!")
			}

			t.Logf("Write Struct Encode/Decode OK")
		}
	}
	*/
}
