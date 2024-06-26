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

package object

import (
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func newResultSet(code uint32, msg string) *kvapi.ResultSet {
	return &kvapi.ResultSet{
		StatusCode:    code,
		StatusMessage: msg,
	}
}

func newResultSetOK() *kvapi.ResultSet {
	return newResultSet(kvapi.Status_OK, "")
}

func newResultSetWithClientError(msg string) *kvapi.ResultSet {
	return newResultSet(kvapi.Status_InvalidArgument, msg)
}
