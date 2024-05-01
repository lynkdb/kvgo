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
	mrand "math/rand"
	"testing"
)

func Test_LogRangeToken(t *testing.T) {
	//
	tk := &logRangeToken{
		version: 1,
	}
	for i := 0; i < 16; i++ {
		tk.data1 = append(tk.data1, uint64(mrand.Int63n(32)))
		tk.data1 = append(tk.data1, uint64(mrand.Int63n(1<<48)))
	}

	t.Logf("tk v %d, raw %v", tk.version, tk.data1)

	//
	token := tk.encode()
	t.Logf("tk str %s, len %d", token, len(token))

	token2 := newLogRangeToken(token)
	if token != token2.encode() {
		t.Fatal("diff fail")
	}
	t.Logf("tk2 v %d, raw %v", token2.version, token2.data1)
}
