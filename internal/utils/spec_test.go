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
	"encoding/json"
	"testing"
)

func Test_ParseJobSpec(t *testing.T) {

	type Sub struct {
		Source string `json:"source_1,omitempty"`
	}

	type Obj struct {
		Name   string   `json:"name"`
		Source string   `json:"source" default_value:"abc"`
		Sub1   *Sub     `json:"sub1"`
		Sub2   Sub      `json:"sub2"`
		Array  []string `json:"array"`
		Bytes  []byte   `json:"bytes"`
		Subs1  []*Sub   `json:"subs1"`
		Subs2  []Sub    `json:"subs2"`
	}

	o := Obj{
		Name: "test",
		Sub1: &Sub{
			Source: "src1",
		},
		Sub2: Sub{
			Source: "src1",
		},
		Array: []string{"a", "b"},
		Bytes: []byte("hello"),
	}

	spec, err := ParseJobSpec(Obj{})
	if err != nil {
		t.Fatal(err)
	}
	js, _ := json.MarshalIndent(spec, "", "  ")
	t.Logf("spec %v", string(js))

	m := TryParseMap(o)
	js, _ = json.MarshalIndent(m, "", "  ")
	t.Logf("map %v", string(js))
}
