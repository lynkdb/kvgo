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
	"errors"

	kv2 "github.com/lynkdb/kvspec/v2"
)

func (cn *Conn) TableList(req *kv2.TableListRequest) *kv2.ObjectResult {

	sReq := &kv2.SysCmdRequest{
		Cmd: &kv2.SysCmdRequest_TableList{
			TableList: req,
		},
	}

	return cn.SysCmd(sReq)
}

func (cn *Conn) TableSet(req *kv2.TableSetRequest) *kv2.ObjectResult {

	if req == nil || !kv2.TableNameReg.MatchString(req.Name) {
		return kv2.NewObjectResultClientError(errors.New("invalid table name"))
	}

	sReq := &kv2.SysCmdRequest{
		Cmd: &kv2.SysCmdRequest_TableSet{
			TableSet: req,
		},
	}

	return cn.SysCmd(sReq)
}