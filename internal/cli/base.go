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
	"sort"
	"strings"
	"sync"

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"
)

var (
	mu      sync.Mutex
	arrCmds []baseCommandAction
	idxCmds = map[string]baseCommandAction{}
)

type baseCommandSpec struct {
	Path string
	Desc string
}

type baseCommandAction interface {
	Spec() baseCommandSpec
	Action(fg flagSet, l *readline.Instance) (string, error)
}

func init() {
	register(new(cmdHelp))
}

func register(fn baseCommandAction) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := idxCmds[fn.Spec().Path]; !ok {
		idxCmds[fn.Spec().Path] = fn
		arrCmds = append(arrCmds, fn)

		sort.Slice(arrCmds, func(i, j int) bool {
			return strings.Compare(arrCmds[i].Spec().Path, arrCmds[j].Spec().Path) < 0
		})
	}
}

type cmdHelp struct{}

func (cmdHelp) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "help",
	}
}

func (cmdHelp) Action(fg flagSet, l *readline.Instance) (string, error) {

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewWriter(&tbuf)
	)

	table.SetRowLine(false)
	table.SetColumnSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.EnableBorder(false)
	table.SetAutoWrapText(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	table.SetHeader([]string{"Command", "Usage"})

	for _, c := range arrCmds {
		if c.Spec().Path == "help" {
			continue
		}
		table.Append([]string{c.Spec().Path, c.Spec().Desc})
	}

	table.Append([]string{"help", ""})
	// table.Append([]string{"quit", ""})

	table.Render()

	return fmt.Sprintf("\n%s\n", tbuf.String()), nil
}

func Invoke(s string, l *readline.Instance) (string, error) {
	mu.Lock()
	defer mu.Unlock()

	fg := flagParse(s)

	for _, c := range arrCmds {
		if !strings.HasPrefix(fg.path, c.Spec().Path) {
			continue
		}

		if fg.path != c.Spec().Path {
			fg.path = strings.TrimSpace(fg.path[len(c.Spec().Path):])
		} else {
			fg.path = ""
		}

		fg.varArgs = flagVarParse(fg.path)

		return c.Action(fg, l)
	}

	return "", fmt.Errorf("no command match")
}

func uptimeFormat(sec int64) string {

	s := ""

	d := (sec / 86400)
	if d > 1 {
		s = fmt.Sprintf("%d days ", d)
	} else if d == 1 {
		s = fmt.Sprintf("%d day ", d)
	}

	sec = sec % 86400
	h := sec / 3600

	sec = sec % 3600
	m := sec / 60

	sec = sec % 60

	s += fmt.Sprintf("%02d:%02d:%02d", h, m, sec)

	return s
}
