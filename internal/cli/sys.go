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

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	register(new(cmdSysInfo))
	register(new(cmdSysGet))
}

type cmdSysInfo struct{}

func (cmdSysInfo) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "sys-info",
		Desc: "sys-info [--all]",
	}
}

func (cmdSysInfo) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.SysGetRequest{
		Name:   "info",
		Params: map[string]string{},
	}

	if _, ok := fg.ValueOK("all"); ok {
		req.Params["all"] = "true"
	}

	rs := adminClient.SysGet(req)
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

	table.SetHeader([]string{"Section", "Detail"})

	table.SetRowLine(true)
	table.SetAutoWrapText(false)
	table.SetCenterSeparator("|")

	for _, kv := range rs.Items {

		var buf bytes.Buffer
		json.Indent(&buf, kv.Value, "", "  ")

		table.Append([]string{
			string(kv.Key),
			buf.String(),
		})
	}

	table.Render()

	return tbuf.String(), nil
}

type cmdSysGet struct{}

func (cmdSysGet) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "sys-get",
		Desc: "sys-get <module name> [-limit N]",
	}
}

func (cmdSysGet) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.SysGetRequest{
		Name:  fg.path,
		Limit: 10,
	}

	if req.Name == "" {
		return "", fmt.Errorf("no <name> found")
	}

	if fg.Value("limit").Int64() > 0 {
		req.Limit = fg.Value("limit").Int64()
	}

	rs := adminClient.SysGet(req)
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

	table.SetHeader([]string{"Key", "Value"})

	table.SetRowLine(true)
	table.SetAutoWrapText(false)

	for _, kv := range rs.Items {

		var buf bytes.Buffer
		json.Indent(&buf, kv.Value, "", "  ")

		table.Append([]string{
			string(kv.Key),
			buf.String(),
		})
	}

	table.Render()

	return tbuf.String(), nil
}
