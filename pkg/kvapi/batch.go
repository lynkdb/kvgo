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

package kvapi

import (
	"errors"
)

func NewBatchRequest() *BatchRequest {
	return &BatchRequest{}
}

func (it *BatchRequest) SetDatabase(name string) *BatchRequest {
	it.Database = name
	return it
}

func (it *BatchRequest) Write(key []byte, value any) *WriteRequest {
	item := NewWriteRequest(key, value)
	it.Items = append(it.Items, &RequestUnion{
		Value: &RequestUnion_Write{
			Write: item,
		},
	})
	return item
}

func (it *BatchRequest) Read(key []byte, keys ...[]byte) *ReadRequest {
	item := NewReadRequest(key, keys...)
	it.Items = append(it.Items, &RequestUnion{
		Value: &RequestUnion_Read{
			Read: item,
		},
	})
	return item
}

func (it *BatchRequest) Delete(key []byte) *DeleteRequest {
	item := NewDeleteRequest(key)
	it.Items = append(it.Items, &RequestUnion{
		Value: &RequestUnion_Delete{
			Delete: item,
		},
	})
	return item
}

func (it *BatchRequest) Range(lowerKey, upperKey []byte) *RangeRequest {
	item := NewRangeRequest(lowerKey, upperKey)
	it.Items = append(it.Items, &RequestUnion{
		Value: &RequestUnion_Range{
			Range: item,
		},
	})
	return item
}

func (it *BatchRequest) Valid() error {

	for _, req := range it.Items {

		if req.Value == nil {
			return errors.New("request empty")
		}

		reqDatabase := ""

		switch req.Value.(type) {
		case *RequestUnion_Write:
			reqDatabase = req.Value.(*RequestUnion_Write).Write.Database

		case *RequestUnion_Delete:
			reqDatabase = req.Value.(*RequestUnion_Delete).Delete.Database

		case *RequestUnion_Read:
			reqDatabase = req.Value.(*RequestUnion_Read).Read.Database

		case *RequestUnion_Range:
			reqDatabase = req.Value.(*RequestUnion_Range).Range.Database

		default:
			return errors.New("invalid request type")
		}

		if it.Database == "" && reqDatabase != "" {
			it.Database = reqDatabase
		} else if reqDatabase != "" && it.Database != reqDatabase {
			return errors.New("all subrequests can only operate on the same database")
		}
	}

	return nil
}
