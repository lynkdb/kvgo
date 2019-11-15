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
	"time"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/sko"
)

func TestObject(t *testing.T) {

	var (
		err  error
		Data sko.ObjectConnector
	)

	test_dir := "/dev/shm/kvgo/data_sko"

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

	// ObjectPut
	ow := Data.NewObjectWriter([]byte("0001")).
		DataValueSet(1, nil)
	if rs := Data.ObjectPut(ow); !rs.OK() {
		t.Fatalf("ObjectPut !OK, Err %s", rs.Message)
	}

	// ObjectQuery Key
	or := Data.NewObjectReader().KeySet([]byte("0001"))
	if rs := Data.ObjectQuery(or); !rs.OK() {
		t.Fatal("ObjectQuery Key !OK")
	} else {
		if rs.DataValue().Uint32() != 1 {
			t.Fatal("ObjectQuery Key !OK Compare")
		}
	}

	// ObjectDel
	if rs := Data.ObjectDel(ow); !rs.OK() {
		t.Fatal("ObjectDel !OK")
	}
	if rs := Data.ObjectQuery(or); !rs.NotFound() {
		t.Fatal("ObjectDel !OK")
	}

	//
	Data.ObjectPut(Data.NewObjectWriter([]byte("0001")).DataValueSet(1, nil))
	Data.ObjectPut(Data.NewObjectWriter([]byte("0002")).DataValueSet(2, nil))
	Data.ObjectPut(Data.NewObjectWriter([]byte("0003")).DataValueSet(3, nil))

	// ObjectQuery KeyRange
	or = Data.NewObjectReader().
		KeyRangeSet([]byte("0001"), []byte("0009")).
		LimitNumSet(10)
	if rs := Data.ObjectQuery(or); !rs.OK() {
		t.Fatal("ObjectQuery !OK")
	} else {

		if len(rs.Items) != 2 {
			t.Fatal("ObjectQuery KeyRange !OK")
		}

		for i, item := range rs.Items {
			if item.DataValue().Int() != (i + 2) {
				t.Fatal("ObjectQuery KeyRange !OK")
			}
		}
	}

	// ObjectQuery KeyRange+RevRange
	or = Data.NewObjectReader().
		KeyRangeSet([]byte("0003"), []byte("0000")).
		RevRangeSet(true).
		LimitNumSet(10)
	if rs := Data.ObjectQuery(or); !rs.OK() {
		t.Fatal("ObjectQuery RevRange !OK")
	} else {

		if len(rs.Items) != 2 {
			t.Fatalf("ObjectQuery RevRange !OK %d", len(rs.Items))
		}

		for i, item := range rs.Items {
			if item.DataValue().Int() != (2 - i) {
				t.Fatal("ObjectQuery RevRange !OK")
			}
		}
	}

	// ObjectPut Expired
	ow = Data.NewObjectWriter([]byte("0001")).
		DataValueSet("ttl", nil).
		ExpireSet(200)
	if rs := Data.ObjectPut(ow); !rs.OK() {
		t.Fatal("ObjectPut !OK Expired")
	}
	or = Data.NewObjectReader().
		KeySet([]byte("0001"))
	if rs := Data.ObjectQuery(or); !rs.OK() {
		t.Fatal("ObjectQuery Key !OK Expired")
	}
	time.Sleep(1e9)
	if rs := Data.ObjectQuery(or); !rs.NotFound() {
		t.Fatal("ObjectQuery Key !OK Expired")
	}

	// ObjectPut Struct
	obj := &sko.ObjectData{
		Attrs: 100,
	}
	ow = Data.NewObjectWriter([]byte("0001")).
		DataValueSet(obj, nil)
	if rs := Data.ObjectPut(ow); !rs.OK() {
		t.Fatal("ObjectPut !OK")
	}
	if rs := Data.ObjectQuery(or); !rs.OK() {
		t.Fatal("ObjectQuery Key !OK")
	} else {
		var item sko.ObjectData
		if err := rs.DataValue().Decode(&item, nil); err != nil {
			t.Fatal("ObjectQuery Key DataValue().Decode() !OK")
		}
		if item.Attrs != 100 {
			t.Fatal("ObjectQuery Key DataValue().Decode() !OK")
		}
	}
}
