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
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"

	"google.golang.org/protobuf/proto"
)

func MetaDecode(bs []byte) (*Meta, int, error) {

	if len(bs) > 2 {
		switch bs[0] {
		case keyValueMetaVersion1:
			if bs[1] > 0 && (int(bs[1])+2) <= len(bs) {
				var meta Meta
				if err := StdProto.Decode(bs[2:(int(bs[1])+2)], &meta); err == nil {
					return &meta, int(bs[1]) + 2, nil
				} else {
					return nil, 0, err
				}
			}
		case keyValueMetaVersion2:
			if len(bs) > 3 {
				if ms := binary.BigEndian.Uint16(bs[1:3]); ms > 0 && int(ms+3) <= len(bs) {
					var meta Meta
					if err := StdProto.Decode(bs[3:int(ms+3)], &meta); err == nil {
						return &meta, int(ms + 3), nil
					}
				}
			}
		}
	}
	return nil, 0, errors.New("invalid meta format")
}

func LogDecode(bs []byte) (*LogMeta, error) {
	var log LogMeta
	err := StdProto.Decode(bs, &log)
	return &log, err
}

func KeyValueDecode(bs []byte) (*KeyValue, error) {

	meta, offset, err := MetaDecode(bs)
	if err != nil {
		return nil, err
	}

	item := &KeyValue{
		Meta: meta,
	}

	if offset < len(bs) {
		item.Value = bs[offset:]
	}

	return item, nil
}

func NewKeyValue(key []byte, meta *Meta) *KeyValue {
	return &KeyValue{
		Meta: meta,
		Key:  key,
	}
}

func (it *KeyValue) Valid() error {

	if len(it.Value) == 0 {
		return errors.New("value not found")
	}

	if it.Meta != nil && it.Meta.Checksum > 0 {
		if it.Meta.Checksum != bytesCrc32Checksum(it.Value) {
			return errors.New("invalid value checksum")
		}
	}

	return nil
}

func (it *KeyValue) StringValue() string {
	return string(it.Value)
}

func (it *KeyValue) Int64Value() int64 {
	if i64, err := strconv.ParseInt(string(it.Value), 10, 64); err == nil {
		return i64
	}
	return 0
}

func (it *KeyValue) Uint64Value() uint64 {
	if u64, err := strconv.ParseUint(string(it.Value), 10, 64); err == nil {
		return u64
	}
	return 0
}

func (it *KeyValue) BoolValue() bool {
	if b, err := strconv.ParseBool(string(it.Value)); err == nil {
		return b
	}
	return false
}

func (it *KeyValue) Float64Value() float64 {
	if f64, err := strconv.ParseFloat(string(it.Value), 64); err == nil {
		return f64
	}
	return 0
}

func (it *KeyValue) Encode(o interface{}, c ValueCodec) error {
	if c == nil || o == nil || len(it.Value) == 0 {
		return errors.New("codec or data not found")
	}
	v, err := c.Encode(o)
	if err != nil {
		return err
	}
	it.Value = v
	return nil
}

func (it *KeyValue) Decode(o interface{}, c ValueCodec) error {
	if c == nil || o == nil || len(it.Value) == 0 {
		return errors.New("codec or data not found")
	}
	return c.Decode(it.Value, o)
}

func (it *KeyValue) JsonDecode(o interface{}) error {
	if len(it.Value) == 0 {
		return errors.New("data not found")
	}
	return json.Unmarshal(it.Value, o)
}

func (it *KeyValue) JsonEncode(o interface{}) error {
	v, err := json.Marshal(o)
	if err != nil {
		return err
	}
	it.Value = v
	return nil
}

type ProtoCodec struct{}

func (ProtoCodec) Encode(obj proto.Message) ([]byte, error) {
	return proto.Marshal(obj)
}

func (ProtoCodec) Decode(bs []byte, obj proto.Message) error {
	return proto.Unmarshal(bs, obj)
}

var StdProto = &ProtoCodec{}
