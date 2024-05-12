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

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	register(new(jobList))
	register(new(jobUpdate))
}

type jobList struct{}

func (jobList) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "job-list",
		Desc: "job-list",
	}
}

func (jobList) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.JobListRequest{}

	rs := adminClient.JobList(req)
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

	table.SetHeader([]string{"#", "Key", "Content"})

	table.SetRowLine(true)
	table.SetAutoWrapText(false)

	for i, kv := range rs.Items {

		var buf bytes.Buffer
		json.Indent(&buf, kv.Value, "", "  ")

		table.Append([]string{
			strconv.Itoa(i + 1),
			string(kv.Key),
			buf.String(),
		})
	}

	table.Render()

	return tbuf.String(), nil
}

type jobUpdate struct{}

func (jobUpdate) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "job-update",
		Desc: "job-update <job-id>",
	}
}

func (jobUpdate) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.ReadRequest{}

	if len(fg.varArgs) > 0 {
		req.Database = fg.varArgs[0]
	} else {
		l.SetPrompt("job-id: ")
		dbName, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.Database = dbName
	}

	if len(fg.varArgs) > 1 {
		req.Keys = append(req.Keys, []byte(fg.varArgs[1]))
	} else {
		l.SetPrompt("key: ")
		key, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.Keys = append(req.Keys, []byte(key))
	}

	rs := kvclient.Read(req)
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

	if len(rs.Items) == 1 {

		table.SetHeader([]string{"Field", "Content"})

		table.SetRowLine(true)
		table.SetAutoWrapText(false)

		kv := rs.Items[0]

		//
		js, _ := json.MarshalIndent(kv.Meta, "", "  ")
		table.Append([]string{"Meta", string(js)})

		//
		table.Append([]string{"Key", string(kv.Key)})

		//
		if len(kv.Value) >= 2 &&
			(kv.Value[0] == '{' || kv.Value[0] == '[') {
			var buf bytes.Buffer
			json.Indent(&buf, kv.Value, "", "  ")
			table.Append([]string{"Value", buf.String()})
		} else {
			table.Append([]string{"Value", string(kv.Value)})
		}
	}

	table.Render()

	return tbuf.String(), nil
}
