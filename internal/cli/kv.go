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
	"fmt"
	"strconv"

	"github.com/chzyer/readline"
	json "github.com/goccy/go-json"
	"github.com/lynkdb/lynkapi/go/lynkcli"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/tidwall/pretty"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	lynkcli.RegisterCommonCommand(new(kvRange))
	lynkcli.RegisterCommonCommand(new(kvRead))
}

func jsonPretty(js []byte) []byte {
	js = pretty.Pretty(js)
	if len(js) > 0 && js[len(js)-1] == '\n' {
		return js[:len(js)-1]
	}
	return js
}

type kvRange struct{}

func (kvRange) Spec() lynkcli.BaseCommandSpec {
	return lynkcli.BaseCommandSpec{
		Path: "kv-range",
		Desc: "kv-range <database name> <lower key> <upper key> [-limit N]",
	}
}

func (kvRange) Action(fg lynkcli.FlagSet, l *readline.Instance) (string, error) {

	req := &kvapi.RangeRequest{}

	if len(fg.VarArgs) == 3 {
		req.Database = fg.VarArgs[0]
		req.LowerKey = []byte(fg.VarArgs[1])
		req.UpperKey = []byte(fg.VarArgs[2])
	} else {

		l.SetPrompt("database name: ")
		dbName, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.Database = dbName

		l.SetPrompt("lower key: ")
		lowerKey, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.LowerKey = []byte(lowerKey)

		l.SetPrompt("upper key: ")
		upperKey, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.UpperKey = []byte(upperKey)
	}

	if fg.Value("limit").Int64() > 0 {
		req.Limit = fg.Value("limit").Int64()
	} else {
		req.Limit = 10
	}

	req.Attrs |= kvapi.Read_Attrs_MetaOnly

	rs := kvclient.Range(req)
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

	table.Options(tablewriter.WithRendition(tw.Rendition{
		Settings: tw.Settings{
			Separators: tw.Separators{BetweenRows: tw.On},
		},
	}))

	table.Header([]string{"#", "Meta", "Key"})

	// table.SetRowLine(true)
	// table.SetAutoWrapText(false)
	// table.EnableBorder(false)

	for i, kv := range rs.Items {

		js, _ := json.Marshal(kv.Meta)

		table.Append([]string{
			strconv.Itoa(i + 1),
			string(jsonPretty(js)),
			string(kv.Key),
		})
	}

	table.Render()

	return tbuf.String(), nil
}

type kvRead struct{}

func (kvRead) Spec() lynkcli.BaseCommandSpec {
	return lynkcli.BaseCommandSpec{
		Path: "kv-read",
		Desc: "kv-read <database name> <key>",
	}
}

func (kvRead) Action(fg lynkcli.FlagSet, l *readline.Instance) (string, error) {

	req := &kvapi.ReadRequest{}

	if len(fg.VarArgs) > 0 {
		req.Database = fg.VarArgs[0]
	} else {
		l.SetPrompt("database name: ")
		dbName, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.Database = dbName
	}

	if len(fg.VarArgs) > 1 {
		req.Keys = append(req.Keys, []byte(fg.VarArgs[1]))
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

		table.Header([]string{"Field", "Content"})

		table.Options(tablewriter.WithRendition(tw.Rendition{
			Settings: tw.Settings{
				Separators: tw.Separators{BetweenRows: tw.On},
			},
		}))

		// table.SetRowLine(true)
		// table.SetAutoWrapText(false)

		kv := rs.Items[0]

		//
		js, _ := json.Marshal(kv.Meta)
		table.Append([]string{"Meta", string(jsonPretty(js))})

		//
		table.Append([]string{"Key", string(kv.Key)})

		//
		if len(kv.Value) >= 2 &&
			(kv.Value[0] == '{' || kv.Value[0] == '[') {
			js := jsonPretty(kv.Value)
			table.Append([]string{"Value", string(js)})
		} else {
			table.Append([]string{"Value", string(kv.Value)})
		}
	}

	table.Render()

	return tbuf.String(), nil
}
