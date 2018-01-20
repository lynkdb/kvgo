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
	"github.com/lynkdb/iomix/skv"
)

func TestProg(t *testing.T) {

	var (
		err  error
		Data skv.Connector
	)

	test_dir := "/dev/shm/kvgo/data_prog"

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

	if rs := Data.ProgPut(skv.NewProgKey("000", uint32(1)),
		skv.NewValueObject(1), nil); !rs.OK() {
		t.Fatal("ProgPut !OK")
	}
	if rs := Data.ProgGet(skv.NewProgKey("000", uint32(1))); !rs.OK() {
		t.Fatal("ProgGet !OK")
	} else {
		if rs.Uint32() != 1 {
			t.Fatal("ProgGet !OK Compare")
		}
	}

	//
	Data.ProgPut(skv.NewProgKey("000", uint32(2)), skv.NewValueObject(2), nil)
	Data.ProgPut(skv.NewProgKey("000", uint32(3)), skv.NewValueObject(3), nil)
	if rs := Data.ProgScan(
		skv.NewProgKey("000", uint32(0)),
		skv.NewProgKey("000", uint32(9)),
		10,
	); !rs.OK() {
		t.Fatal("ProgScan !OK")
	} else {

		if rs.KvLen() != 3 {
			t.Fatal("ProgScan !OK")
		}
		if k, v := rs.KvEntry(0); v == nil || v.Uint64() != 1 {
			t.Fatalf("ProgScan !OK %s    /    %s", string(k[:]), v.String())
		}
		if _, v := rs.KvEntry(1); v == nil || v.Uint64() != 2 {
			t.Fatal("ProgScan !OK")
		}
		if _, v := rs.KvEntry(2); v == nil || v.Uint64() != 3 {
			t.Fatal("ProgScan !OK")
		}
	}

	if rs := Data.ProgRevScan(
		skv.NewProgKey("000", uint32(0)),
		skv.NewProgKey("000", uint32(9)),
		10,
	); !rs.OK() {
		t.Fatal("ProgRevScan !OK")
	} else {

		if rs.KvLen() != 3 {
			t.Fatal("ProgRevScan !OK")
		}
		if _, v := rs.KvEntry(0); v == nil || v.Uint64() != 3 {
			t.Fatal("ProgRevScan !OK")
		}
		if _, v := rs.KvEntry(1); v == nil || v.Uint64() != 2 {
			t.Fatal("ProgRevScan !OK")
		}
		if _, v := rs.KvEntry(2); v == nil || v.Uint64() != 1 {
			t.Fatal("ProgRevScan !OK")
		}
	}

	//
	if rs := Data.ProgPut(skv.NewProgKey("000", uint32(2)), skv.NewValueObject("22"), &skv.ProgWriteOptions{
		Actions: skv.ProgOpMetaSum | skv.ProgOpMetaSize | skv.ProgOpFoldMeta,
	}); !rs.OK() {
		t.Fatal("ProgPut !OK Options")
	}
	if rs := Data.ProgGet(skv.NewProgKey("000", uint32(2))); !rs.OK() {
		t.Fatal("ProgGet !OK")
	} else {
		if meta := rs.Meta(); meta == nil {
			t.Fatal("ProgGet !OK Meta")
		} else {
			if meta.Size != 2 {
				t.Fatal("ProgGet !OK Meta.Size")
			}
			if meta.Expired > 0 {
				t.Fatal("ProgGet !OK Meta.Expired")
			}
		}
	}

	if rs := Data.ProgGet(skv.NewProgKey("000", "")); !rs.OK() {
		t.Fatal("ProgGet !OK")
	} else {
		if meta := rs.Meta(); meta == nil {
			t.Fatal("ProgGet !OK Meta")
		} else {
			if meta.Num == 0 {
				t.Fatal("ProgGet !OK Meta.Num")
			}
		}
	}

	// Expired
	if rs := Data.ProgPut(skv.NewProgKey("ttl", "key"), skv.NewValueObject("22"),
		&skv.ProgWriteOptions{
			Expired: time.Now().UTC().Add(1 * time.Second),
		}); !rs.OK() {
		t.Fatal("ProgPut !OK Expired")
	}
	if rs := Data.ProgGet(skv.NewProgKey("ttl", "key")); !rs.OK() {
		t.Fatal("ProgGet !OK Expired")
	} else {
		if meta := rs.Meta(); meta == nil {
			t.Fatal("ProgGet !OK Meta.Expired")
		} else {
			if meta.Expired < uint64(time.Now().UTC().UnixNano()) {
				t.Fatal("ProgGet !OK Meta.Expired")
			}
		}
	}
	time.Sleep(2e9)
	if rs := Data.ProgGet(skv.NewProgKey("ttl", "key")); !rs.NotFound() {
		t.Fatal("ProgGet !OK Expired")
	}

}
