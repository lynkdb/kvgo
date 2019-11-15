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
	"time"

	"github.com/lessos/lessgo/types"
	"github.com/lynkdb/iomix/sko"
	"github.com/lynkdb/iomix/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	ttl_worker_sleep = 200e6
	ttl_worker_limit = 200
)

func (cn *Conn) ttl_worker() {

	go func() {

		for {

			batch := new(leveldb.Batch)
			ls := cn.rawScan(
				t_ns_cat(ns_ttl, uint64_to_bytes(0)),
				t_ns_cat(ns_ttl, uint64_to_bytes(uint64(types.MetaTimeNow()))),
				ttl_worker_limit,
			)
			for i := 1; i < len(ls.Items); i += 2 {
				batch.Delete(ls.Items[i-1].Data)
				if len(ls.Items[i-1].Data) > 9 {
					batch.Delete(t_ns_cat(ns_meta, ls.Items[i-1].Data[9:]))
					batch.Delete(ls.Items[i-1].Data[9:])
				}
			}
			cn.db.Write(batch, nil)

			if len(ls.Items)/2 < ttl_worker_limit {
				time.Sleep(ttl_worker_sleep)
			}
		}
	}()

	go func() {

		for {

			var (
				batch = new(leveldb.Batch)
				iter  = cn.db.NewIterator(&util.Range{
					Start: t_ns_cat(ns_sko_ttl, uint64_to_bytes(0)),
					Limit: t_ns_cat(ns_sko_ttl, uint64_to_bytes(uint64(time.Now().UnixNano()/1e6))),
				}, nil)
				num = 0
			)

			for iter.Next() {

				meta, err := sko.ObjectMetaDecode(bytes_clone(iter.Value()))
				if err != nil {
					break
				}

				data, err := cn.db.Get(t_ns_cat(ns_sko_meta, meta.Key), nil)
				if err == nil {

					cmeta, err := sko.ObjectMetaDecode(data)
					if err != nil {
						break
					}

					if cmeta.Version == meta.Version {
						batch.Delete(t_ns_cat(ns_sko_meta, meta.Key))
						batch.Delete(t_ns_cat(ns_sko_data, meta.Key))
						if meta.Version > 0 {
							batch.Delete(t_ns_cat(ns_sko_log, uint64_to_bytes(meta.Version)))
						}
					}

				} else if err.Error() != "leveldb: not found" {
					break
				}

				batch.Delete(keyExpireEncode(ns_sko_ttl, meta.Expired, meta.Key))
				num += 1

				if num >= ttl_worker_limit {
					break
				}
			}

			iter.Release()

			if num > 0 {
				cn.db.Write(batch, nil)
			}

			if num < ttl_worker_limit {
				time.Sleep(ttl_worker_sleep)
			}
		}
	}()

	go func() {

		for {

			time_cut := uint64(time.Now().UTC().UnixNano())

			batch := new(leveldb.Batch)
			ls := cn.rawScan(
				t_ns_cat(ns_prog_ttl, uint64_to_bytes(0)),
				t_ns_cat(ns_prog_ttl, uint64_to_bytes(time_cut)),
				ttl_worker_limit,
			)
			for i := 1; i < len(ls.Items); i += 2 {

				batch.Delete(ls.Items[i-1].Data)

				if len(ls.Items[i-1].Data) < 10 {
					continue
				}

				if ls.Items[i-1].Data[9] < ns_prog_def || ls.Items[i-1].Data[9] > ns_prog_cut {
					continue
				}

				rs := cn.rawGet(ls.Items[i-1].Data[9:])
				if !rs.OK() {
					continue
				}
				meta := rs.Meta()
				if meta == nil {
					continue
				}

				if meta.Expired < prog_ttl_zero ||
					meta.Expired > time_cut {
					continue
				}

				batch.Delete(ls.Items[i-1].Data[9:])

				if meta.Num == 1 {

					if pk := skv.KvProgKeyDecode(ls.Items[i-1].Data[9:]); pk != nil {
						if pmeta := cn.rawGet(pk.EncodeFoldMeta(ls.Items[i-1].Data[9])).Meta(); pmeta != nil {
							if pmeta.Num <= 1 {
								cn.rawDel(pk.EncodeFoldMeta(ls.Items[i-1].Data[9]))
							} else {
								pmeta.Num--

								if pmeta.Size > uint64(len(rs.Data)-1) {
									pmeta.Size -= uint64(len(rs.Data) - 1)
								} else {
									pmeta.Size = 0
								}

								if bs := pmeta.Encode(); len(bs) > 1 {
									cn.rawPut(pk.EncodeFoldMeta(ls.Items[i-1].Data[9]), bs, 0)
								}
							}
						}
					}
				}
			}
			cn.db.Write(batch, nil)

			if len(ls.Items)/2 < ttl_worker_limit {
				time.Sleep(ttl_worker_sleep)
			}
		}
	}()
}
