// Copyright 2015 lynkdb Authors, All rights reserved.
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
	"os/exec"
	"testing"

	"code.hooto.com/lynkdb/iomix/connect"
	"code.hooto.com/lynkdb/iomix/iotypes"
	"code.hooto.com/lynkdb/iomix/skv"
)

var (
	err  error
	Data skv.Connector
)

func TestDataType(t *testing.T) {

	test_dir := "/dev/shm/kvgo/data"

	exec.Command("mkdir", "-p", test_dir).Output()
	if _, err := exec.Command("/usr/bin/rm", "-rf", test_dir).Output(); err != nil {
		t.Fatal(err)
	}

	opts := connect.ConnOptions{}
	opts.SetValue("data_dir", test_dir)

	Data, err = Open(opts)
	if err != nil {
		t.Fatalf("Can Not Connect To %s, Error: %s", "database", err.Error())
	}
	defer Data.Close()

	if rs := Data.KvGet([]byte("file1")); rs.OK() || !rs.NotFound() {
		t.Fatal("KvGet !NotFound")
	}

	if rs := Data.KvPut([]byte("file1"), "abc", nil); !rs.OK() {
		t.Fatal("KvPut !OK")
	}

	if rs := Data.KvDel([]byte("file1")); !rs.OK() {
		t.Fatal("KvDel !OK")
	}
	if rs := Data.KvGet([]byte("file1")); !rs.NotFound() {
		t.Fatal("KvDel !OK" + rs.Bytex().String())
	}

	Data.KvPut([]byte("file1"), "abc", nil)
	Data.KvPut([]byte("file2"), []byte("def"), nil)
	if rs := Data.KvGet([]byte("file1")); !rs.OK() || rs.Bytex().String() != "abc" {
		t.Fatal("KvGet !OK")
	}
	if rs := Data.KvGet([]byte("file2")); !rs.OK() || rs.Bytex().String() != "def" {
		t.Fatal("KvGet !OK")
	}

	if rs := Data.KvScan([]byte("file"), []byte("file"), 10); !rs.OK() || rs.KvLen() != 2 {
		t.Fatal("KvScan !OK")
	} else {

		rs.KvEach(func(entry *skv.ResultEntry) int {

			switch string(entry.Key) {

			case "file1":
				if entry.Bytex().String() != "abc" {
					t.Fatal("KvScan !OK")
				}

			case "file2":
				if entry.Bytex().String() != "def" {
					t.Fatal("KvScan !OK")
				}

			default:
				t.Fatal("KvScan !OK")
			}

			return 0
		})
	}

	if rs := Data.KvIncrby([]byte("incr"), 10); !rs.OK() || rs.Int64() != 10 {
		t.Fatal("KvIncrby !OK")
	}

	if rs := Data.KvIncrby([]byte("incr"), -20); !rs.OK() || rs.Int64() != -10 {
		t.Fatal("KvIncrby !OK")
	}

	// Json Encode/Decode
	jsitem := object_test{
		Code:    "Code",
		Message: "Message",
	}
	jslen := 0
	if rs := Data.KvPut([]byte("object"), jsitem, nil); !rs.OK() {
		t.Fatal("KvPut Object !OK")
	}
	if rs := Data.KvGet([]byte("object")); !rs.OK() {
		t.Fatal("KvGet Object !OK")
	} else {
		var jsitem2 object_test
		if err := rs.Decode(&jsitem2); err != nil {
			t.Fatal(err)
		}
		if jsitem2.Code != "Code" {
			t.Fatal("KvGet Object/JsonDecode !OK")
		}
		jslen = len(rs.Data[0])
		if jslen < 10 {
			t.Fatalf("KvGet Object/ProtoBuf !OK len:%d", jslen)
		}
	}

	// ProtoBuf Encode/Decode
	pbitem := &iotypes.ErrorMeta{
		Code:    "Code",
		Message: "Message",
	}
	if rs := Data.KvPut([]byte("objectpb"), pbitem, nil); !rs.OK() {
		t.Fatal("KvPut ObjectPB !OK")
	}
	if rs := Data.KvGet([]byte("objectpb")); !rs.OK() {
		t.Fatal("KvGet ObjectPB !OK")
	} else {
		var pbitem2 iotypes.ErrorMeta
		if err := rs.Decode(&pbitem2); err != nil {
			t.Fatal(err)
		}
		if pbitem2.Code != "Code" || pbitem2.Message != "Message" {
			t.Fatal("KvGet Object/ProtoBuf !OK")
		}
		if f := float32(len(rs.Data[0])) / float32(jslen); f > 0.5 {
			t.Fatalf("KvGet Object/ProtoBuf !OK compress rate %f", f)
		}
	}

}

type object_test struct {
	Code    string
	Message string
}
