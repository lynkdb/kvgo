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
	"context"
	"errors"
	"time"

	kv2 "github.com/lynkdb/kvspec/v2"
)

func (cn *Conn) SysCmd(rr *kv2.SysCmdRequest) *kv2.ObjectResult {

	if len(cn.opts.Cluster.Masters) > 0 {

		if cn.opts.ClientConnectEnable {
			return cn.sysCmdRemote(rr)
		}

		rs, err := cn.public.SysCmd(nil, rr)
		if err != nil {
			return kv2.NewObjectResultServerError(err)
		}
		return rs
	}

	return cn.sysCmdLocal(rr)
}

func (cn *Conn) sysCmdLocal(rr *kv2.SysCmdRequest) *kv2.ObjectResult {

	if rr.Cmd == nil {
		return kv2.NewObjectResultClientError(errors.New("cmd not found"))
	}

	var rs *kv2.ObjectResult

	switch {

	case rr.GetTableSet() != nil:

		cmdReq := rr.GetTableSet()

		if !kv2.TableNameReg.MatchString(cmdReq.Name) {
			rs = kv2.NewObjectResultClientError(errors.New("invalid table name"))
		} else {

			rr2 := kv2.NewObjectWriter(nsSysTable(cmdReq.Name), &kv2.TableItem{
				Name: cmdReq.Name,
				Desc: cmdReq.Desc,
			}).IncrNamespaceSet(sysTableIncrNS).
				TableNameSet(sysTableName)

			tdb := cn.tabledb(cmdReq.Name)
			if tdb == nil {
				rr2.ModeCreateSet(true)

			}

			rs = cn.Commit(rr2)
			if rs.OK() {
				if tdb == nil && rs.Meta.IncrId > 0 {
					cn.dbTableSetup(cmdReq.Name, uint32(rs.Meta.IncrId))
				}
			}
		}

	case rr.GetTableList() != nil:

		rr2 := kv2.NewObjectReader(nil).
			TableNameSet(sysTableName).
			KeyRangeSet(nsSysTable(""), append(nsSysTable(""), 0xff)).
			LimitNumSet(1000)

		if rs = cn.Query(rr2); rs.OK() {

			if rs2 := cn.Query(
				rr2.KeyRangeSet(nsSysTableStatus(""), append(nsSysTableStatus(""), 0xff))); rs2.OK() {

				statuses := map[string]*kv2.TableStatus{}
				for _, v := range rs2.Items {
					var item kv2.TableStatus
					if err := v.DataValue().Decode(&item, nil); err == nil {
						statuses[item.Name] = &item
					}
				}

				for _, v := range rs.Items {
					var item kv2.TableItem
					if err := v.DataValue().Decode(&item, nil); err == nil {
						if st := statuses[item.Name]; st != nil {
							item.Status = st
							v.DataValueSet(item, nil)
						}
					}
				}
			}
		}

	default:
		rs = kv2.NewObjectResultClientError(errors.New("cmd not found"))
	}

	return rs
}

func (cn *Conn) sysCmdRemote(rr *kv2.SysCmdRequest) *kv2.ObjectResult {

	if rr.Cmd == nil {
		return kv2.NewObjectResultClientError(errors.New("cmd not found"))
	}

	masters := cn.opts.Cluster.randMasters(3)
	if len(masters) < 1 {
		return kv2.NewObjectResultClientError(errors.New("no master found"))
	}

	for _, v := range masters {

		conn, err := clientConn(v.Addr, cn.authKey(v.Addr), v.AuthTLSCert)
		if err != nil {
			continue
		}

		ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
		defer fc()

		rs, err := kv2.NewPublicClient(conn).SysCmd(ctx, rr)
		if err != nil {
			return kv2.NewObjectResultServerError(err)
		}

		return rs
	}

	return kv2.NewObjectResultServerError(errors.New("no cluster nodes"))
}

func (it *PublicServiceImpl) SysCmd(ctx context.Context, req *kv2.SysCmdRequest) (*kv2.ObjectResult, error) {

	if ctx != nil {
		if err := appAuthValid(ctx, it.db.serverKey); err != nil {
			return kv2.NewObjectResultClientError(err), nil
		}
	}

	if len(it.db.opts.Cluster.Masters) == 0 {
		return it.db.SysCmd(req), nil
	}

	rs := kv2.NewObjectResultOK()

	return rs, nil
}
