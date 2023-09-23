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

package server

import (
	"fmt"

	"github.com/lynkdb/kvgo/pkg/kvapi"
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

func newResultSetWithNotFound(msg string) *kvapi.ResultSet {
	return newResultSet(kvapi.Status_NotFound, msg)
}

func newResultSetWithServerError(msg string, args ...interface{}) *kvapi.ResultSet {
	if len(args) > 0 {
		return newResultSet(kvapi.Status_ServerError, fmt.Sprintf(msg, args...))
	}
	return newResultSet(kvapi.Status_ServerError, msg)
}

func newResultSetWithConflict(msg string) *kvapi.ResultSet {
	return newResultSet(kvapi.Status_Conflict, msg)
}

func newResultSetWithVersionConflict(msg string) *kvapi.ResultSet {
	return newResultSet(kvapi.Status_VersionConflict, msg)
}

func newResultSetWithAuthDeny(msg string) *kvapi.ResultSet {
	return newResultSet(kvapi.Status_AuthDeny, msg)
}

func newBatchResponse(code uint32, msg string) *kvapi.BatchResponse {
	return &kvapi.BatchResponse{
		StatusCode:    code,
		StatusMessage: msg,
	}
}

func resultSetAppend(rs *kvapi.ResultSet, key []byte, meta *kvapi.Meta) (*kvapi.KeyValue, error) {
	item := &kvapi.KeyValue{
		Key:  key,
		Meta: meta,
	}
	rs.Items = append(rs.Items, item)
	return item, nil
}

func resultSetAppendWithJsonObject(
	rs *kvapi.ResultSet, key []byte, meta *kvapi.Meta, obj interface{},
) (*kvapi.KeyValue, error) {
	item := &kvapi.KeyValue{
		Key:  key,
		Meta: meta,
	}
	if err := item.JsonEncode(obj); err != nil {
		return nil, err
	}
	rs.Items = append(rs.Items, item)
	return item, nil
}
