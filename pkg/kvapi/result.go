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
	"bytes"
	"errors"
	"fmt"
)

func (it *ResultSet) OK() bool {
	return it.StatusCode == Status_OK
}

func (it *ResultSet) NotFound() bool {
	return it.StatusCode == Status_NotFound
}

func (it *ResultSet) Error() error {
	if it.StatusCode == Status_OK {
		return nil
	}
	return errors.New(it.StatusMessage)
}

func (it *ResultSet) ErrorMessage() string {
	return fmt.Sprintf("#%d %s", it.StatusCode, it.StatusMessage)
}

func (it *ResultSet) Meta() *Meta {
	if len(it.Items) > 0 {
		return it.Items[0].Meta
	}
	return nil
}

func (it *ResultSet) Item() *KeyValue {
	if len(it.Items) > 0 {
		return it.Items[0]
	}
	return &KeyValue{}
}

func (it *ResultSet) JsonDecode(o interface{}) error {
	if len(it.Items) > 0 {
		return it.Items[0].JsonDecode(o)
	}
	return errors.New("no data found")
}

func (it *ResultSet) Decode(o interface{}, c ValueCodec) error {
	if len(it.Items) > 0 {
		return it.Items[0].Decode(o, c)
	}
	return errors.New("no data found")
}

func (it *ResultSet) Lookup(key []byte) *KeyValue {
	for _, v := range it.Items {
		if bytes.Compare(key, v.Key) == 0 {
			return v
		}
	}
	return &KeyValue{}
}

func (it *BatchResponse) OK() bool {
	return it.StatusCode == Status_OK
}

func (it *BatchResponse) NotFound() bool {
	return it.StatusCode == Status_NotFound
}

func (it *BatchResponse) Error() error {
	if it.StatusCode == Status_OK {
		return nil
	}
	return errors.New(it.StatusMessage)
}

func (it *BatchResponse) ErrorMessage() string {
	return fmt.Sprintf("#%d %s", it.StatusCode, it.StatusMessage)
}

func (it *LogRangeResponse) OK() bool {
	return it.Status.Code == Status_OK
}

func (it *LogRangeResponse) NotFound() bool {
	return it.Status.Code == Status_NotFound
}

func (it *LogRangeResponse) Error() error {
	if it.Status.Code == Status_OK {
		return nil
	}
	return errors.New(it.Status.Message)
}

func (it *LogRangeResponse) ErrorMessage() string {
	return fmt.Sprintf("#%d %s", it.Status.Code, it.Status.Message)
}
