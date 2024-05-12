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

package utils

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"google.golang.org/protobuf/types/known/structpb"
)

func TryParseMap(obj interface{}) map[string]*structpb.Value {

	m := &structpb.Struct{
		Fields: map[string]*structpb.Value{},
	}

	if obj == nil {
		return m.Fields
	}

	if reflect.TypeOf(obj).Kind() != reflect.Struct {
		return m.Fields
	}

	var (
		parseStruct func(up *structpb.Struct, rv reflect.Value)
		parseArray  func(up *structpb.ListValue, rv reflect.Value)
	)

	parseStruct = func(up *structpb.Struct, rv reflect.Value) {

		if !rv.IsValid() || rv.Type().Kind() != reflect.Struct {
			return
		}

		fields := reflect.VisibleFields(rv.Type())

		for _, fd := range fields {

			if fd.Name == "" || fd.Name[0] < 'A' || fd.Name[0] > 'Z' {
				continue
			}

			var (
				name = fd.Name
				v1   = rv.FieldByIndex(fd.Index)
				v2   *structpb.Value
			)

			if sa := strings.Split(fd.Tag.Get("json"), ","); len(sa) > 0 &&
				sa[0] != "" && sa[0] != "omitempty" {
				name = sa[0]
			}

			switch v1.Kind() {

			case reflect.String:
				v2 = structpb.NewStringValue(v1.String())

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v2 = structpb.NewNumberValue(float64(v1.Int()))

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v2 = structpb.NewNumberValue(float64(v1.Uint()))

			case reflect.Float32, reflect.Float64:
				v2 = structpb.NewNumberValue(v1.Float())

			case reflect.Bool:
				v2 = structpb.NewBoolValue(v1.Bool())

			case reflect.Struct:
				st := &structpb.Struct{}
				v2 = structpb.NewStructValue(st)
				parseStruct(st, v1)

			case reflect.Pointer:
				st := &structpb.Struct{}
				v2 = structpb.NewStructValue(st)
				parseStruct(st, v1)

			case reflect.Slice:
				if v1.Len() > 0 {
					if v1.Index(0).Kind() == reflect.Uint8 {
						b64 := base64.StdEncoding.EncodeToString(v1.Bytes())
						v2 = structpb.NewStringValue(b64)
					} else {
						ls := &structpb.ListValue{}
						v2 = structpb.NewListValue(ls)
						parseArray(ls, v1)
					}
				}
			}

			if v2 != nil {
				if up.Fields == nil {
					up.Fields = map[string]*structpb.Value{}
				}
				up.Fields[name] = v2
			}
		}
	}

	parseArray = func(up *structpb.ListValue, rv reflect.Value) {

		if !rv.IsValid() || rv.Type().Kind() != reflect.Slice {
			return
		}

		n := rv.Cap()
		for i := 0; i < n; i++ {

			var (
				v1 = rv.Index(i)
				v2 *structpb.Value
			)

			switch v1.Kind() {

			case reflect.String:
				v2 = structpb.NewStringValue(v1.String())

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v2 = structpb.NewNumberValue(float64(v1.Int()))

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v2 = structpb.NewNumberValue(float64(v1.Uint()))

			case reflect.Float32, reflect.Float64:
				v2 = structpb.NewNumberValue(v1.Float())

			case reflect.Bool:
				v2 = structpb.NewBoolValue(v1.Bool())

			case reflect.Struct:
				st := &structpb.Struct{}
				v2 = structpb.NewStructValue(st)
				parseStruct(st, v1)

			case reflect.Pointer:
				st := &structpb.Struct{}
				v2 = structpb.NewStructValue(st)
				parseStruct(st, v1)

			case reflect.Slice:
				ls := &structpb.ListValue{}
				v2 = structpb.NewListValue(ls)
				parseArray(ls, v1)
			}

			if v2 != nil {
				up.Values = append(up.Values, v2)
			}
		}
	}

	parseStruct(m, reflect.ValueOf(obj))

	return m.Fields
}

func ParseJobSpec(obj interface{}) (*kvapi.JobSpec, error) {

	if obj == nil {
		return nil, fmt.Errorf("empty object")
	}

	rt := reflect.TypeOf(obj)
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid object type")
	}

	var (
		spec        = &kvapi.JobSpec{}
		parseStruct func(spec *kvapi.JobSpec, upOpt *kvapi.JobSpec_Option, rt reflect.Type)
		parseArray  func(upOpt *kvapi.JobSpec_Option, rt reflect.Type)
	)

	parseDefaultValue := func(v string, t string) *structpb.Value {
		if v == "" {
			return nil
		}
		switch t {
		case kvapi.JobSpec_String:
			return structpb.NewStringValue(v)

		case kvapi.JobSpec_Int:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil && i != 0 {
				return structpb.NewNumberValue(float64(i))
			}

		case kvapi.JobSpec_Uint:
			if i, err := strconv.ParseUint(v, 10, 64); err == nil && i != 0 {
				return structpb.NewNumberValue(float64(i))
			}

		case kvapi.JobSpec_Float:
			if f, err := strconv.ParseFloat(v, 64); err == nil && f != 0 {
				return structpb.NewNumberValue(f)
			}

		case kvapi.JobSpec_Bool:
			if b, err := strconv.ParseBool(v); err == nil && b == true {
				return structpb.NewBoolValue(b)
			}
		}

		return nil
	}

	parseStruct = func(spec *kvapi.JobSpec, upOpt *kvapi.JobSpec_Option, rt reflect.Type) {

		if rt.Kind() != reflect.Struct {
			return
		}

		fields := reflect.VisibleFields(rt)

		for _, fd := range fields {

			if fd.Name == "" || fd.Name[0] < 'A' || fd.Name[0] > 'Z' {
				continue
			}

			var (
				opt = &kvapi.JobSpec_Option{
					Name: fd.Name,
				}
				// v = rv.FieldByIndex(fd.Index)
			)

			if s := fd.Tag.Get("json"); s != "" {
				sa := strings.Split(s, ",")
				if sa[0] != "" && sa[0] != "omitempty" {
					opt.Name = sa[0]
				}
			}

			switch fd.Type.Kind() {

			case reflect.String:
				opt.Type = kvapi.JobSpec_String

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				opt.Type = kvapi.JobSpec_Int

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				opt.Type = kvapi.JobSpec_Uint

			case reflect.Float32, reflect.Float64:
				opt.Type = kvapi.JobSpec_Float

			case reflect.Bool:
				opt.Type = kvapi.JobSpec_Bool

			case reflect.Struct:
				opt.Type = kvapi.JobSpec_Struct
				parseStruct(nil, opt, fd.Type)

			case reflect.Pointer:
				opt.Type = kvapi.JobSpec_Struct
				parseStruct(nil, opt, fd.Type.Elem())

			case reflect.Slice:
				if fd.Type.Elem().Kind() == reflect.Uint8 {
					opt.Type = kvapi.JobSpec_Bytes
				} else {
					parseArray(opt, fd.Type.Elem())
				}
			}

			if opt.Type != "" {
				opt.DefaultValue = parseDefaultValue(fd.Tag.Get("default_value"), opt.Type)
				if spec != nil {
					spec.Options = append(spec.Options, opt)
				} else if upOpt != nil {
					upOpt.Options = append(upOpt.Options, opt)
				}
			}
		}
	}

	parseArray = func(opt *kvapi.JobSpec_Option, rt reflect.Type) {

		switch rt.Kind() {

		case reflect.String:
			opt.Type = kvapi.JobSpec_ArrayString

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			opt.Type = kvapi.JobSpec_ArrayInt

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			opt.Type = kvapi.JobSpec_ArrayUint

		case reflect.Float32, reflect.Float64:
			opt.Type = kvapi.JobSpec_ArrayFloat

		case reflect.Bool:
			opt.Type = kvapi.JobSpec_ArrayBool

		case reflect.Struct:
			opt.Type = kvapi.JobSpec_ArrayStruct
			parseStruct(nil, opt, rt)

		case reflect.Pointer:
			if rt.Elem().Kind() == reflect.Struct {
				opt.Type = kvapi.JobSpec_ArrayStruct
				parseStruct(nil, opt, rt.Elem())
			}
		}
	}

	parseStruct(spec, nil, rt)

	return spec, nil
}
