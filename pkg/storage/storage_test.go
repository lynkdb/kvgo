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

package storage_test

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/goleveldb"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
)

/** v2
storage samples 1000000
goos: darwin
goarch: arm64
pkg: github.com/lynkdb/kvgo/pkg/storage
Benchmark_Storage_SeqRead-8      	 4526296	      2698 ns/op
Benchmark_Storage_RandRead-8     	 1885909	      6346 ns/op
Benchmark_Storage_RangeRead-8    	  756844	     15413 ns/op
Benchmark_Storage_SeqWrite-8     	 1401904	      8431 ns/op
Benchmark_Storage_RandWrite-8    	 1000000	     10729 ns/op
Benchmark_Storage_BatchWrite-8   	  781740	     16703 ns/op
*/

/** v3
storage samples 1000000
goos: darwin
goarch: arm64
pkg: github.com/lynkdb/kvgo/pkg/storage
Benchmark_Storage_SeqRead-8      	 4318741	      2728 ns/op
Benchmark_Storage_RandRead-8     	  770739	     14674 ns/op
Benchmark_Storage_RangeRead-8    	  541857	     28112 ns/op
Benchmark_Storage_SeqWrite-8     	 2224464	      6198 ns/op
Benchmark_Storage_RandWrite-8    	 1541120	      7666 ns/op
Benchmark_Storage_BatchWrite-8   	  785190	     12780 ns/op
*/

var (
	testmu     sync.Mutex
	testDriver = "v2"
)

func Test_Common(t *testing.T) {
	db, err := testConnOpen(testDriver, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer testConnClean(db)

	var (
		key0   = []byte("0000")
		value0 = []byte("0000")

		key1   = []byte("0001")
		value1 = []byte("0001")
	)

	if rs := db.Put(key0, value0, nil); !rs.OK() {
		t.Fatal(rs.Error())
	} else if rs2 := db.Get(key0, nil); !rs2.OK() {
		t.Fatal(rs2.Error())
	} else if bytes.Compare(rs2.Bytes(), value0) != 0 {
		t.Fatal("cmp fail")
	}

	if rs := db.Get(key1, nil); !rs.NotFound() {
		t.Fatal("not-found fail")
	}

	if rs := db.Delete(key0, nil); !rs.OK() {
		t.Fatal(rs.Error())
	}

	if rs := db.Get(key0, nil); !rs.NotFound() {
		t.Fatal("not-found fail")
	}

	{
		batch := db.NewBatch()
		batch.Put(key0, value0)
		if rs := batch.Apply(&storage.WriteOptions{}); !rs.OK() {
			t.Fatal(rs.Error())
		} else if rs2 := db.Get(key0, nil); !rs2.OK() {
			t.Fatal(rs2.Error())
		}
		batch.Clear()

		batch.Delete(key0)
		batch.Put(key1, value1)
		if rs := batch.Apply(&storage.WriteOptions{}); !rs.OK() {
			t.Fatal(rs.Error())
		} else if batch.Len() != 2 {
			t.Fatal("len fail")
		} else {
			if rs2 := db.Get(key0, nil); !rs2.NotFound() {
				t.Fatal(rs2.Error())
			}
			if rs2 := db.Get(key1, nil); !rs2.OK() {
				t.Fatal(rs2.Error())
			} else if bytes.Compare(rs2.Bytes(), value1) != 0 {
				t.Fatal("cmp fail")
			}
		}
	}

	{
		batch := db.NewBatch()
		num := 10
		for i := 0; i < num; i++ {
			batch.Put([]byte(fmt.Sprintf("iter-%010d", i)), []byte(fmt.Sprintf("value-%d", i)))
		}
		if rs := batch.Apply(&storage.WriteOptions{}); !rs.OK() {
			t.Fatal(rs.Error())
		}

		iter, _ := db.NewIterator(&storage.IterOptions{
			LowerKey: []byte("iter-"),
			UpperKey: []byte("iter-z"),
		})

		n := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if bytes.Compare(iter.Value(), []byte(fmt.Sprintf("value-%d", n))) != 0 {
				t.Fatalf("iter cmp fail #%d", n)
			} else {
				// t.Logf("iter cmp #%d", n)
			}
			n += 1
		}
		if n != num {
			t.Fatalf("iter fail %d : %d", n, num)
		}
		iter.Release()

		iter, _ = db.NewIterator(&storage.IterOptions{
			LowerKey: []byte("iter-"),
			UpperKey: []byte("iter-z"),
		})

		for iter.SeekToLast(); iter.Valid(); iter.Prev() {
			n -= 1
			if bytes.Compare(iter.Value(), []byte(fmt.Sprintf("value-%d", n))) != 0 {
				t.Fatalf("iter cmp fail #%d %s", n, string(iter.Key()))
			} else {
				// t.Logf("iter cmp #%d", n)
			}
		}
		if n != 0 {
			t.Fatalf("iter fail #%d", n)
		}
	}

	{ // API::SizeOf
		value := make([]byte, 1<<10)
		for i := 0; i < 100000; i++ {
			db.Put([]byte(fmt.Sprintf("size-of-key-%d", i)), value, nil)
		}
		db.Flush()

		if rs, err := db.SizeOf([]*storage.IterOptions{
			{LowerKey: []byte{}, UpperKey: []byte("zzzz")},
		}); err != nil {
			t.Fatal(err)
		} else if len(rs) != 1 || rs[0] < 1 {
			t.Fatal("size-of")
		} else {
			t.Logf("size of %d", rs[0])
		}
	}

	{ // API::KeyStats
		var (
			num   = 100 + mrand.Intn(100)
			value = make([]byte, 1<<10)
		)
		for i := 0; i < num; i++ {
			db.Put([]byte(fmt.Sprintf("key-stat-%d", i)), value, nil)
		}
		db.Flush()

		if rs, err := db.KeyStats(&storage.IterOptions{
			LowerKey: []byte("key-stat-"), UpperKey: []byte("key-stat-z"),
		}); err != nil {
			t.Fatal(err)
		} else if rs.Keys != int64(num) {
			t.Fatalf("key-stats %d : %d", rs.Keys, num)
		} else {
			t.Logf("key-stats %d", rs.Keys)
		}
	}
}

func Benchmark_Storage_SeqRead(b *testing.B) {
	benchmarkStorageSeqRead(b, testDriver, 1e6)
}

func Benchmark_Storage_RandRead(b *testing.B) {
	benchmarkStorageRandRead(b, testDriver, 1e6)
}

func Benchmark_Storage_RangeRead(b *testing.B) {
	benchmarkStorageRangeRead(b, testDriver, 1e6)
}

func Benchmark_Storage_SeqWrite(b *testing.B) {
	benchmarkStorageSeqWrite(b, testDriver)
}

func Benchmark_Storage_RandWrite(b *testing.B) {
	benchmarkStorageRandWrite(b, testDriver)
}

func Benchmark_Storage_BatchWrite(b *testing.B) {
	benchmarkStorageBatchWrite(b, testDriver)
}

func testConnOpen(drvname string, samples int) (storage.Conn, error) {

	testmu.Lock()
	defer testmu.Unlock()

	testDir := "/dev/shm"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
	}

	if samples > 0 {
		testDir = filepath.Clean(fmt.Sprintf("%s/kvgo-test/data-%d", testDir, samples))
	} else {
		testDir = filepath.Clean(fmt.Sprintf("%s/kvgo-test/data", testDir))
	}

	db, err := storage.Open(drvname, &storage.Options{
		DataDirectory:   testDir,
		WriteBufferSize: 16,
	})
	if err != nil {
		return nil, err
	}
	if samples > 0 {
		if rs := db.Get([]byte(fmt.Sprintf("%032d", samples-1)), nil); rs.NotFound() {
			for i := 0; i < samples; i++ {
				bs := randBytes(128 + mrand.Intn(256)) // size 128 ~ 384 bytes, avg 256 bytes
				if rs := db.Put([]byte(fmt.Sprintf("%032d", i)), bs, nil); !rs.OK() {
					return nil, rs.Error()
				}
			}
			fmt.Println("storage samples", samples)
		}
	}
	return db, nil
}

func testConnClean(conn storage.Conn) {
	testmu.Lock()
	defer testmu.Unlock()
	conn.Close()
	testDir := "/dev/shm"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
	}
	testDir = filepath.Clean(fmt.Sprintf("%s/kvgo-test/data", testDir))
	exec.Command("rm", "-rf", testDir).Output()
}

func benchmarkStorageSeqRead(b *testing.B, drvname string, samples int) {
	db, err := testConnOpen(drvname, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%032d", i%samples))
		if rs := db.Get(key, nil); !rs.OK() && !rs.NotFound() {
			b.Fatalf("Get ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRandRead(b *testing.B, drvname string, samples int) {
	db, err := testConnOpen(drvname, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%032d", mrand.Intn(samples+(samples/10))))
		if rs := db.Get(key, nil); !rs.OK() && !rs.NotFound() {
			b.Fatalf("Get ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRangeRead(b *testing.B, drvname string, samples int) {
	db, err := testConnOpen(drvname, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var (
			offseti = mrand.Intn(samples - 10)
			offset  = []byte(fmt.Sprintf("%032d", offseti))
			cutset  = []byte(fmt.Sprintf("%032d", offseti+10))
		)
		iter, _ := db.NewIterator(&storage.IterOptions{
			UpperKey: offset,
			LowerKey: cutset,
		})
		for ok := iter.SeekToFirst(); ok; ok = iter.Next() {
			if bytes.Compare(iter.Key(), cutset) >= 0 {
				break
			}
		}
	}
}

func benchmarkStorageSeqWrite(b *testing.B, drvname string) {
	db, err := testConnOpen(drvname, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs := randBytes(100 + mrand.Intn(900))
		if rs := db.Put([]byte(fmt.Sprintf("%032d", i)), bs, nil); !rs.OK() {
			b.Fatalf("Put ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRandWrite(b *testing.B, drvname string) {
	db, err := testConnOpen(drvname, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs := randBytes(100 + mrand.Intn(900))
		if rs := db.Put([]byte(fmt.Sprintf("%032d", mrand.Int())), bs, nil); !rs.OK() {
			b.Fatalf("Put ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageBatchWrite(b *testing.B, drvname string) {
	db, err := testConnOpen(drvname, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer testConnClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := mrand.Int()
		batch := db.NewBatch()
		batch.Put([]byte(fmt.Sprintf("log-%032d", i)), randBytes(20+mrand.Intn(50)))
		batch.Put([]byte(fmt.Sprintf("meta-%032d", id)), randBytes(20+mrand.Intn(50)))
		batch.Put([]byte(fmt.Sprintf("data-%032d", id)), randBytes(100+mrand.Intn(900)))
		if rs := batch.Apply(&storage.WriteOptions{}); rs.Error() != nil {
			b.Fatalf("batch apply ER!, Err %v", rs.Error())
		}
	}
}

func randBytes(size int) []byte {

	if size < 1 {
		size = 1
	} else if size > 2000000 {
		size = 2000000
	}

	bs := make([]byte, size)

	if _, err := crand.Read(bs); err != nil {
		for i := range bs {
			bs[i] = uint8(mrand.Intn(256))
		}
	}

	return bs
}
