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

package cli

import (
	"errors"
	"fmt"

	"github.com/chzyer/readline"

	"github.com/lynkdb/kvgo"
	"github.com/lynkdb/kvgo/pkg/kvapi"
)

func TableCreate(l *readline.Instance) (string, error) {

	l.SetPrompt("table name: ")
	tableName, err := l.Readline()
	if err != nil {
		return "", err
	}

	req := &kvapi.TableCreateRequest{
		Name:   tableName,
		Engine: kvgo.StorageEngineV2,
	}

	rs := adminClient.TableCreate(req)
	if !rs.OK() {
		return "", errors.New(rs.Message)
	}

	return fmt.Sprintf("OK table %s, id %d", tableName, rs.Meta.IncrId), nil
}
