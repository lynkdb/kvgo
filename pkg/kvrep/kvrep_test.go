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

package kvrep_test

import (
	"os/exec"
	"testing"

	"github.com/lynkdb/kvgo/v2/internal/tests"
	"github.com/lynkdb/kvgo/v2/pkg/kvrep"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func Test_KV_Replica(t *testing.T) {

	dir := "/tmp/pkg_kvrep_test"
	exec.Command("rm", "-rf", dir+"/*").Output()

	db, err := kvrep.NewReplica(&storage.Options{
		DataDirectory: dir,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		db.Close()
		exec.Command("rm", "-rf", dir).Output()
	}()

	tests.ClientAPI(db, t)
}
