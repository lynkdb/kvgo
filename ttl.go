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
	"time"

	"github.com/lessos/lessgo/types"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	ttl_worker_sleep = 500e6
	ttl_worker_limit = 1000
)

func (cn *Conn) ttl_worker() {

	go func() {

		for {

			ls := cn.RawScan(
				t_ns_cat(ns_ttl, uint64_to_bytes(0)),
				t_ns_cat(ns_ttl, uint64_to_bytes(uint64(types.MetaTimeNow()))),
				ttl_worker_limit,
			).KvList()

			for _, v := range ls {

				batch := new(leveldb.Batch)

				batch.Delete(t_ns_cat(ns_meta, v.Key[9:]))
				batch.Delete(v.Key[9:])
				batch.Delete(v.Key)

				cn.db.Write(batch, nil)
			}

			if len(ls) < ttl_worker_limit {
				time.Sleep(ttl_worker_sleep)
			}
		}
	}()
}
