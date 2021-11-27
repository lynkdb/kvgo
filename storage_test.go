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
	"bytes"
	"fmt"
	mrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

var (
	storTestMu sync.Mutex
)

func storOpen(drv kv2.StorageEngineOpen, samples int) (kv2.StorageEngine, error) {

	storTestMu.Lock()
	defer storTestMu.Unlock()

	if drv == nil {
		drv = storageLevelDBOpen
	}

	testDir := "/dev/shm"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
	}

	if samples > 0 {
		testDir = filepath.Clean(fmt.Sprintf("%s/kvgo_test/stor_%d", testDir, samples))
	} else {
		testDir = filepath.Clean(fmt.Sprintf("%s/kvgo_test/stor", testDir))
	}

	db, err := drv(testDir, &kv2.StorageOptions{
		WriteBufferSize: 16,
		MaxTableSize:    8,
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

func storClean(stor kv2.StorageEngine) {
	storTestMu.Lock()
	defer storTestMu.Unlock()
	stor.Close()
	testDir := "/dev/shm"
	if runtime.GOOS == "darwin" {
		testDir, _ = os.UserHomeDir()
	}
	testDir = filepath.Clean(fmt.Sprintf("%s/kvgo_test/stor", testDir))
	exec.Command("rm", "-rf", testDir).Output()
	exec.Command("rm", "-rf", testDir+"_pebble").Output()
}

func benchmarkStorageSeqRead(b *testing.B, drv kv2.StorageEngineOpen, samples int) {
	db, err := storOpen(drv, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%032d", i%samples))
		if rs := db.Get(key, nil); !rs.OK() && !rs.NotFound() {
			b.Fatalf("Commit ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRandRead(b *testing.B, drv kv2.StorageEngineOpen, samples int) {
	db, err := storOpen(drv, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%032d", mrand.Intn(samples+(samples/10))))
		if rs := db.Get(key, nil); !rs.OK() && !rs.NotFound() {
			b.Fatalf("Commit ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRangeRead(b *testing.B, drv kv2.StorageEngineOpen, samples int) {
	db, err := storOpen(drv, samples)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var (
			offseti = mrand.Intn(samples - 10)
			offset  = []byte(fmt.Sprintf("%032d", offseti))
			cutset  = []byte(fmt.Sprintf("%032d", offseti+10))
		)
		iter := db.NewIterator(&kv2.StorageIteratorRange{
			Start: offset,
			Limit: cutset,
		})
		for ok := iter.First(); ok; ok = iter.Next() {
			if bytes.Compare(iter.Key(), cutset) >= 0 {
				break
			}
		}
	}
}

func benchmarkStorageSeqWrite(b *testing.B, drv kv2.StorageEngineOpen) {
	db, err := storOpen(drv, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs := randBytes(100 + mrand.Intn(900))
		if rs := db.Put([]byte(fmt.Sprintf("%032d", i)), bs, nil); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageRandWrite(b *testing.B, drv kv2.StorageEngineOpen) {
	db, err := storOpen(drv, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bs := randBytes(100 + mrand.Intn(900))
		if rs := db.Put([]byte(fmt.Sprintf("%032d", mrand.Int())), bs, nil); !rs.OK() {
			b.Fatalf("Commit ER!, Err %s", rs.ErrorMessage())
		}
	}
}

func benchmarkStorageBatchWrite(b *testing.B, drv kv2.StorageEngineOpen) {
	db, err := storOpen(drv, 0)
	if err != nil {
		b.Fatalf("Can Not Open Database %s", err.Error())
	}
	defer storClean(db)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := mrand.Int()
		batch := db.NewBatch()
		batch.Put([]byte(fmt.Sprintf("log-%032d", i)), randBytes(20+mrand.Intn(50)))
		batch.Put([]byte(fmt.Sprintf("meta-%032d", id)), randBytes(20+mrand.Intn(50)))
		batch.Put([]byte(fmt.Sprintf("data-%032d", id)), randBytes(100+mrand.Intn(900)))
		if err := batch.Commit(); err != nil {
			b.Fatalf("Commit ER!, Err %s", err.Error())
		}
	}
}

//
func Benchmark_Storage_LevelDB_SeqRead(b *testing.B) {
	benchmarkStorageSeqRead(b, storageLevelDBOpen, 100000000)
}

func Benchmark_Storage_Pebble_SeqRead(b *testing.B) {
	benchmarkStorageSeqRead(b, storagePebbleOpen, 100000000)
}

//
func Benchmark_Storage_LevelDB_RandRead(b *testing.B) {
	benchmarkStorageRandRead(b, storageLevelDBOpen, 100000000)
}

func Benchmark_Storage_Pebble_RandRead(b *testing.B) {
	benchmarkStorageRandRead(b, storagePebbleOpen, 100000000)
}

//
func Benchmark_Storage_LevelDB_RangeRead(b *testing.B) {
	benchmarkStorageRangeRead(b, storageLevelDBOpen, 100000000)
}

func Benchmark_Storage_Pebble_RangeRead(b *testing.B) {
	benchmarkStorageRangeRead(b, storagePebbleOpen, 100000000)
}

//
func Benchmark_Storage_LevelDB_SeqWrite(b *testing.B) {
	benchmarkStorageSeqWrite(b, storageLevelDBOpen)
}

func Benchmark_Storage_Pebble_SeqWrite(b *testing.B) {
	benchmarkStorageSeqWrite(b, storagePebbleOpen)
}

//
func Benchmark_Storage_LevelDB_RandWrite(b *testing.B) {
	benchmarkStorageRandWrite(b, storageLevelDBOpen)
}

func Benchmark_Storage_Pebble_RandWrite(b *testing.B) {
	benchmarkStorageRandWrite(b, storagePebbleOpen)
}

//
func Benchmark_Storage_LevelDB_BatchWrite(b *testing.B) {
	benchmarkStorageBatchWrite(b, storageLevelDBOpen)
}

func Benchmark_Storage_Pebble_BatchWrite(b *testing.B) {
	benchmarkStorageBatchWrite(b, storagePebbleOpen)
}
