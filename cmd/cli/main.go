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

package main

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/chzyer/readline"

	"github.com/lynkdb/kvgo/v2/internal/cli"
)

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func resetPrompt(l *readline.Instance) {
	l.SetPrompt("kvgo cli : ")
}

var (
	version = ""
)

func main() {

	if err := cli.Setup(); err != nil {
		log.Fatal(err)
	}

	l, err := readline.NewEx(&readline.Config{
		AutoComplete:        nil, // completer,
		HistoryFile:         fmt.Sprintf("~/.kvgo_history"),
		InterruptPrompt:     "^C",
		EOFPrompt:           "exit",
		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		resetPrompt(l)

		line, err := l.Readline()

		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		out, err := cli.Invoke(strings.TrimSpace(line), l)

		if err != nil {
			fmt.Println("Error:", err)
		} else if out != "" {
			fmt.Println(out)
		}
	}
}
