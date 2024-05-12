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
	"fmt"
	"testing"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/client"
)

func Test_Client(t *testing.T) {

	ak := server.NewSystemAccessKey()
	ak.Id = "00000000"
	ak.Secret = "f7HzVuCwOaqvUcsFN5AYGLGYNEfubi47tHF607Jh"

	cc := &client.Config{
		Addr:      fmt.Sprintf("127.0.0.1:%d", 9566),
		AccessKey: ak,
	}

	c, err := cc.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
	}
}
