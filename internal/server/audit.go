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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

type auditLogWriter struct {
	mu  sync.Mutex
	dir string
	fp  *os.File

	kv *dbReplica
}

type auditLogEntry struct {
	Time    string      `json:"time"`
	Name    string      `json:"name"`
	Content interface{} `json:"content"`
	File    string      `json:"file"`
}

var (
	flocker sync.Mutex
)

func (it *auditLogWriter) Put(name string, args ...interface{}) {

	if len(args) == 0 {
		return
	}

	_, fileName, lineNum, _ := runtime.Caller(1)
	if n := strings.LastIndex(fileName, "/"); n > 0 {
		fileName = fileName[n+1:]
	}

	item := auditLogEntry{
		Time: time.Now().Format("20060102.150405.000"),
		Name: name,
		File: fmt.Sprintf("%s.%d", fileName, lineNum),
	}

	if len(args) > 1 && args[0] != nil {
		if _, ok := args[0].(string); ok {
			item.Content = fmt.Sprintf(args[0].(string), args[1:]...)
		}
	} else {
		item.Content = args[0]
	}

	bs, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return
	}

	if testLocalMode {
		// testPrintf(string(bs))
	}

	it.write(bs)
}

func (w *auditLogWriter) write(bs []byte) {

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp == nil {
		fp, err := w.open()
		if err != nil {
			return
		}
		w.fp = fp
	}

	if w.fp != nil {
		if _, err := w.fp.Write(bs); err != nil {
			w.fp.Sync()
			w.fp = nil
		}
	}

	if w.kv != nil {
		w.kv.Write(kvapi.NewWriteRequest(nsSysAuditLog(true), bs))
	}
}

func (w *auditLogWriter) Close() {

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp == nil {
		return
	}

	w.fp.Sync()
	w.fp.Close()
	w.fp = nil
}

func (w *auditLogWriter) open() (*os.File, error) {

	flocker.Lock()
	defer flocker.Unlock()

	if len(w.dir) < 1 {
		return nil, errors.New("No -audit_log_dir Setup")
	}

	if _, err := os.Stat(w.dir); err != nil {
		return nil, err
	}

	fp, err := os.OpenFile(w.dir+"/audit-log.log",
		os.O_RDWR|os.O_CREATE, 0644)
	// os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	fp.Seek(0, 0)
	fp.Truncate(0)

	return fp, nil
}
