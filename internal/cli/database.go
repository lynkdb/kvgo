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
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func init() {
	register(new(dbList))
	register(new(dbCreate))
	register(new(dbUpdate))
}

type dbList struct{}

func (dbList) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db list",
	}
}

func (dbList) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.DatabaseListRequest{}

	rs := adminClient.DatabaseList(req)
	if !rs.OK() {
		return "", rs.Error()
	}
	if len(rs.Items) == 0 {
		return "", fmt.Errorf("no response")
	}

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewWriter(&tbuf)
	)

	table.SetHeader([]string{"Name", "Spec", "Updated"})

	table.SetRowLine(true)
	table.SetAutoWrapText(false)
	// table.EnableBorder(false)

	for _, kv := range rs.Items {

		var buf bytes.Buffer
		json.Indent(&buf, kv.Value, "", "  ")

		table.Append([]string{
			string(kv.Key),
			buf.String(),
			time.Unix(kv.Meta.Updated/1e3, 0).Format(time.DateTime),
		})
	}

	table.Render()

	return tbuf.String(), nil
}

type dbCreate struct{}

func (dbCreate) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db create",
	}
}

func (dbCreate) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.DatabaseCreateRequest{
		Engine:     storage.DefaultDriver,
		ReplicaNum: 1,
	}

	l.SetPrompt("database name: ")
	dbName, err := l.Readline()
	if err != nil {
		return "", err
	}
	req.Name = dbName

	l.SetPrompt("replica num (range 1~5, default 1): ")
	sn, err := l.Readline()
	if err != nil {
		return "", err
	}
	if i, err := strconv.Atoi(sn); err == nil && i > 1 && i <= 5 {
		req.ReplicaNum = uint32(i)
	}

	rs := adminClient.DatabaseCreate(req)
	if !rs.OK() {
		return "", rs.Error()
	}

	return fmt.Sprintf("OK database %s, id %d", dbName, rs.Meta().IncrId), nil
}

type dbUpdate struct{}

func (dbUpdate) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db update",
	}
}

func (dbUpdate) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.DatabaseUpdateRequest{}

	l.SetPrompt("database name: ")
	dbName, err := l.Readline()
	if err != nil {
		return "", err
	}
	req.Name = dbName

	l.SetPrompt("description: ")
	req.Desc, err = l.Readline()
	if err != nil {
		return "", err
	}

	rs := adminClient.DatabaseUpdate(req)
	if !rs.OK() {
		return "", rs.Error()
	}

	return fmt.Sprintf("OK database %s, id %d", dbName, rs.Meta().IncrId), nil
}
