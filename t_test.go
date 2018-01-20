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

package kvgo

import (
	"os/exec"
	"testing"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/skv"
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
				if entry.String() != "abc" {
					t.Fatal("KvScan !OK")
				}

			case "file2":
				if entry.String() != "def" {
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
		Version: 400,
		Name:    "Message",
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
		if jsitem2.Version != 400 {
			t.Fatal("KvGet Object/JsonDecode !OK")
		}
		jslen = len(rs.Bytes())
		if jslen < 10 {
			t.Fatalf("KvGet Object/ProtoBuf !OK len:%d", jslen)
		}
	}

	// ProtoBuf Encode/Decode
	pbitem := &skv.ValueMeta{
		Version: 400,
		Name:    "Message",
	}
	if rs := Data.KvPut([]byte("objectpb"), pbitem, nil); !rs.OK() {
		t.Fatal("KvPut ObjectPB !OK")
	}
	if rs := Data.KvGet([]byte("objectpb")); !rs.OK() {
		t.Fatal("KvGet ObjectPB !OK")
	} else {
		var pbitem2 skv.ValueMeta
		if err := rs.Decode(&pbitem2); err != nil {
			t.Fatal(err)
		}
		if pbitem2.Version != 400 || pbitem2.Name != "Message" {
			t.Fatal("KvGet Object/ProtoBuf !OK")
		}
		// if f := float32(len(rs.Data[0])) / float32(jslen); f > 0.5 {
		// 	t.Fatalf("KvGet Object/ProtoBuf !OK compress rate %f", f)
		// }
	}

	/*
		// Path/Object
		if rs := Data.PoNew("po/name", 1, "1111", nil); rs.OK() {
			t.Fatal("PoNew !OK")
		}

		if rs := Data.PoNew("po/name", uint32(1), "1111", nil); !rs.OK() {
			t.Fatal("PoNew !OK")
		}

		if rs := Data.PoGet("po/name", uint32(1)); !rs.OK() {
			t.Fatal("PoGet !OK")
		} else if rs.Bytex().String() != "1111" {
			t.Fatal("PoGet !OK")
		}

		if rs := Data.PoPut("po/name", uint32(1), "2222", nil); !rs.OK() {
			t.Fatal("PoPut !OK")
		}

		if rs := Data.PoGet("po/name", uint32(1)); !rs.OK() {
			t.Fatal("PoGet !OK")
		} else if rs.Bytex().String() != "2222" {
			t.Fatal("PoGet !OK")
		}

		Data.PoPut("po/name", uint32(1), "1111", nil)
		Data.PoPut("po/name", uint32(2), "2222", nil)

		if rs := Data.PoScan("po/name", 0, 10, 10); !rs.OK() || rs.KvLen() != 2 {
			t.Fatal("PoScan !OK")
		} else {

			rs.KvEach(func(entry *skv.ResultEntry) int {

				if len(entry.Key) != 4 {
					t.Fatal("PoScan|PoPut !OK")
					return 1
				}

				switch uint8(entry.Key[3]) {

				case 1:
					if entry.Bytex().String() != "1111" {
						t.Fatal("PoScan !OK")
					}

				case 2:
					if entry.Bytex().String() != "2222" {
						t.Fatal("PoScan !OK")
					}

				default:
					t.Fatal("PoScan !OK")
				}

				return 0
			})
		}

		if rs := Data.PoDel("po/name", uint32(1), nil); !rs.OK() {
			t.Fatal("PoDel !OK")
		}
		if rs := Data.PoGet("po/name", uint32(1)); !rs.NotFound() {
			t.Fatal("PoDel !OK")
		}
	*/
}

type object_test struct {
	Version uint64
	Name    string
}
