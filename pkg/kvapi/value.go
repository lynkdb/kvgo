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
	"encoding/json"
	// "google.golang.org/protobuf/proto"
)

// const (
// 	ValueType_Bytes uint32 = 0
// 	ValueType_Json  uint32 = 1
// 	// ValueType_Protobuf uint8 = 10
// )

// type ValueBytes []byte

type ValueCodec interface {
	Encode(object interface{}) ([]byte, error)
	Decode(value []byte, object interface{}) error
}

var (
	JsonValueCodec jsonValueCodec
)

type jsonValueCodec struct{}

func (jsonValueCodec) Encode(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

func (jsonValueCodec) Decode(value []byte, object interface{}) error {
	return json.Unmarshal(value, object)
}

// type stdValueBytesCodec struct{}

// var (
// 	stdValueCodec = &stdValueBytesCodec{}
// )

/**
func (v ValueBytes) Bytes() []byte {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		return v[1:]
	}
	return nil
}

func (v ValueBytes) String() string {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		return string(v[1:])
	}
	return ""
}

func (v ValueBytes) Int() int {
	return int(v.Int64())
}

func (v ValueBytes) Int8() int8 {
	return int8(v.Int64())
}

func (v ValueBytes) Int16() int16 {
	return int16(v.Int64())
}

func (v ValueBytes) Int32() int32 {
	return int32(v.Int64())
}

func (v ValueBytes) Int64() int64 {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		if i64, err := strconv.ParseInt(string(v[1:]), 10, 64); err == nil {
			return i64
		}
	}
	return 0
}

func (v ValueBytes) Uint() uint {
	return uint(v.Uint64())
}

func (v ValueBytes) Uint8() uint8 {
	return uint8(v.Uint64())
}

func (v ValueBytes) Uint16() uint16 {
	return uint16(v.Uint64())
}

func (v ValueBytes) Uint32() uint32 {
	return uint32(v.Uint64())
}

func (v ValueBytes) Uint64() uint64 {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		if u64, err := strconv.ParseUint(string(v[1:]), 10, 64); err == nil {
			return u64
		}
	}

	return 0
}

func (v ValueBytes) Bool() bool {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		if b, err := strconv.ParseBool(string(v[1:])); err == nil {
			return b
		}
	}
	return false
}

func (v ValueBytes) Float64() float64 {
	if len(v) > 1 && v[0] == ValueType_Bytes {
		if f64, err := strconv.ParseFloat(string(v[1:]), 64); err == nil {
			return f64
		}
	}
	return 0
}

func (v ValueBytes) Decode(object interface{}, opts ...interface{}) error {

	var codec ValueCodec
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		switch opt.(type) {
		case ValueCodec:
			codec = opt.(ValueCodec)
		}
	}

	if codec == nil {
		codec = stdValueCodec
	}

	return codec.Decode(v, object)
}
*/

// func (it *stdValueBytesCodec) Decode(value []byte, object interface{}) error {
//
// 	if len(value) > 1 && object != nil {
//
// 		switch uint32(value[0]) {
//
// 		// case ValueType_Protobuf:
// 		// 	if obj, ok := object.(proto.Message); ok {
// 		// 		if err := StdProto.Decode(value[1:], obj); err != nil {
// 		// 			return errors.New("Invalid Value/ProtoBuf " + err.Error())
// 		// 		}
// 		// 		return nil
// 		// 	}
//
// 		case ValueType_Json:
// 			return json.Unmarshal(value[1:], object)
//
// 		default:
// 			if value[1] == '{' || value[1] == '[' {
// 				return json.Unmarshal(value[1:], object)
// 			}
// 		}
// 	}
//
// 	return errors.New("Invalid Value/Version")
// }
//
// func (it *stdValueBytesCodec) Encode(value interface{}) ([]byte, error) {
//
// 	var valueEnc []byte
//
// 	switch value.(type) {
//
// 	case []byte:
// 		valueEnc = append([]byte{uint8(ValueType_Bytes)}, value.([]byte)...)
//
// 	case string:
// 		valueEnc = append([]byte{uint8(ValueType_Bytes)}, []byte(value.(string))...)
//
// 	//
// 	case uint:
// 		valueEnc = valueEncodeUint(uint64(value.(uint)))
//
// 	case uint8:
// 		valueEnc = valueEncodeUint(uint64(value.(uint8)))
//
// 	case uint16:
// 		valueEnc = valueEncodeUint(uint64(value.(uint16)))
//
// 	case uint32:
// 		valueEnc = valueEncodeUint(uint64(value.(uint32)))
//
// 	case uint64:
// 		valueEnc = valueEncodeUint(value.(uint64))
//
// 	//
// 	case int:
// 		valueEnc = valueEncodeInt(int64(value.(int)))
//
// 	case int8:
// 		valueEnc = valueEncodeInt(int64(value.(int8)))
//
// 	case int16:
// 		valueEnc = valueEncodeInt(int64(value.(int16)))
//
// 	case int32:
// 		valueEnc = valueEncodeInt(int64(value.(int32)))
//
// 	case int64:
// 		valueEnc = valueEncodeInt(value.(int64))
//
// 	//
// 	// case proto.Message:
// 	// 	if bs, err := proto.Marshal(value.(proto.Message)); err != nil {
// 	// 		return nil, errors.New("Invalid ProtoBuf " + err.Error())
// 	// 	} else {
// 	// 		valueEnc = append([]byte{ValueType_Protobuf}, bs...)
// 	// 	}
//
// 	//
// 	case map[string]interface{}, struct{}, interface{}:
// 		if bs, err := json.Marshal(value); err != nil {
// 			return nil, errors.New("encoding JSON fail: " + err.Error())
// 		} else {
// 			valueEnc = append([]byte{uint8(ValueType_Json)}, bs...)
// 		}
//
// 	default:
// 		return nil, errors.New("invalid value type")
// 	}
//
// 	return valueEnc, nil
// }
//
// func valueEncodeUint(num uint64) []byte {
// 	return append([]byte{uint8(ValueType_Bytes)}, []byte(strconv.FormatUint(num, 10))...)
// }
//
// func valueEncodeInt(num int64) []byte {
// 	return append([]byte{uint8(ValueType_Bytes)}, []byte(strconv.FormatInt(num, 10))...)
// }
