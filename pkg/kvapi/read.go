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
	"fmt"
)

func NewReadRequest(key []byte, keys ...[]byte) *ReadRequest {
	req := &ReadRequest{
		Keys: [][]byte{key},
	}
	for _, k := range keys {
		req.Keys = append(req.Keys, k)
	}
	return req
}

func (it *ReadRequest) Valid() error {

	if len(it.Keys) == 0 {
		return errors.New("no key(s) request")
	}

	for i, k := range it.Keys {
		if len(k) == 0 {
			return fmt.Errorf("key (#%d) cannot be empty", i)
		}
	}

	return nil
}

func (it *ReadRequest) SetAttrs(attrs uint64) *ReadRequest {
	it.Attrs |= attrs
	return it
}

func (it *ReadRequest) SetMetaOnly(b bool) *ReadRequest {
	if b {
		it.Attrs = AttrAppend(it.Attrs, Read_Attrs_MetaOnly)
	} else {
		it.Attrs = AttrRemove(it.Attrs, Read_Attrs_MetaOnly)
	}
	return it
}

func NewRangeRequest(lowerKey, upperKey []byte) *RangeRequest {
	req := &RangeRequest{
		LowerKey: lowerKey,
		UpperKey: upperKey,
		Limit:    10,
	}
	return req
}

func (it *RangeRequest) SetLimit(v int64) *RangeRequest {
	it.Limit = v
	return it
}

func (it *RangeRequest) SetRevert(b bool) *RangeRequest {
	it.Revert = b
	return it
}
