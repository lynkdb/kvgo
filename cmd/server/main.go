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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo/internal/server"
)

var (
	version = "git"
	release = "1"
)

func main() {

	srv, err := server.Setup(version, release)
	if err != nil {
		hlog.Printf("error", "%s config err %s", server.AppName, err.Error())
		hlog.Flush()
		os.Exit(1)
	}

	quit := make(chan os.Signal, 2)

	//
	signal.Notify(quit,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL)
	sg := <-quit

	srv.Close()

	hlog.Printf("warn", "kvgo-server signal quit %s", sg.String())
	hlog.Flush()

	time.Sleep(1e9)
}
