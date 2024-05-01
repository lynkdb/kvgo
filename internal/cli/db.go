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
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
)

func init() {
	register(new(dbList))
	register(new(dbCreate))
	register(new(dbUpdate))
	register(new(dbInfo))
	register(new(dbTestData))
}

type dbList struct{}

func (dbList) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db-list",
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
		Path: "db-create",
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

	l.SetPrompt("size size (range 1~512GB, default 8GB): ")
	ss, err := l.Readline()
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(ss, "GB") {
		if i, err := strconv.Atoi(ss[:len(ss)-2]); err == nil && i >= 1 && i <= 512 {
			req.ShardSize = int64(i) << 10
		}
	} else if strings.HasSuffix(ss, "MB") {
		if i, err := strconv.Atoi(ss[:len(ss)-2]); err == nil && i >= 32 && i <= 1024 {
			req.ShardSize = int64(i)
		}
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
		Path: "db-update",
		Desc: "db-update <database name>",
	}
}

func (dbUpdate) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.DatabaseUpdateRequest{}

	if len(fg.varArgs) > 0 {
		req.Name = fg.varArgs[0]
	} else {
		l.SetPrompt("database name: ")
		dbName, err := l.Readline()
		if err != nil {
			return "", err
		}
		req.Name = dbName
	}

	l.SetPrompt("description: ")
	req.Desc, err = l.Readline()
	if err != nil {
		return "", err
	}

	l.SetPrompt("size size (range 1~512GB, default 8GB): ")
	ss, err := l.Readline()
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(ss, "GB") {
		if i, err := strconv.Atoi(ss[:len(ss)-2]); err == nil && i >= 1 && i <= 512 {
			req.ShardSize = int64(i) << 10
		}
	} else if strings.HasSuffix(ss, "MB") {
		if i, err := strconv.Atoi(ss[:len(ss)-2]); err == nil && i >= 32 && i <= 1024 {
			req.ShardSize = int64(i)
		}
	}

	rs := adminClient.DatabaseUpdate(req)
	if !rs.OK() {
		return "", rs.Error()
	}

	return fmt.Sprintf("OK database %s, id %d", req.Name, rs.Meta().IncrId), nil
}

type dbInfo struct{}

func (dbInfo) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db-info",
		Desc: "db-info <database name>",
	}
}

func (dbInfo) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.SysGetRequest{
		Name:  "db-info",
		Limit: 10000,
	}

	var dbName string

	if len(fg.varArgs) > 0 {
		dbName = fg.varArgs[0]
	} else {

		l.SetPrompt("database name: ")
		dbName, err = l.Readline()
		if err != nil {
			return "", err
		}
	}

	req.Params = map[string]string{
		"db_name": dbName,
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

		dbMap    kvapi.DatabaseMap
		dbStatus kvapi.DatabaseMapStatus
	)

	table.SetHeader([]string{
		"Shard", "Action", "Ver", "Updated",
		"Rep ID", "Rep Action", "Rep Store", "Ver",
		"Offset"})

	table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3, 7})
	table.SetCenterSeparator("|")
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetRowLine(true)
	table.SetAutoWrapText(false)

	if err := rs.Lookup([]byte(fmt.Sprintf("db/%s/map", dbName))).JsonDecode(&dbMap); err != nil {
		return "", err
	}

	if err := rs.Lookup([]byte(fmt.Sprintf("db/%s/status", dbName))).JsonDecode(&dbStatus); err != nil {
		return "", err
	}

	if dbStatus.Replicas == nil {
		dbStatus.Replicas = map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	}

	for _, shard := range dbMap.Shards {

		cols := []string{
			fmt.Sprintf("%d -> %d", shard.Prev, shard.Id),
			server.ShardActionDisplay(shard.Action),
			fmt.Sprintf("%d", shard.Version),
			time.Unix(shard.Updated, 0).Format(time.DateTime),
		}

		attrs := []string{
			string(shard.LowerKey),
		}

		if len(attrs[0]) == 0 {
			attrs[0] = "-"
		}

		for _, rep := range shard.Replicas {

			var (
				statusAction uint64
				statusUsed   int64
				statusVer    uint64
			)

			if repStatus, ok := dbStatus.Replicas[rep.Id]; ok {
				statusAction = repStatus.Action
				statusUsed = repStatus.Used
				statusVer = repStatus.LogVersion
			}

			var idArrow string
			if rep.Prev > 0 {
				idArrow = fmt.Sprintf("%d -> %d", rep.Prev, rep.Id)
			} else {
				idArrow = fmt.Sprintf("%d", rep.Id)
			}

			table.Append(append(append(cols, []string{
				idArrow,
				server.ReplicaActionDisplay(rep.Action) + " : " +
					server.ReplicaActionDisplay(statusAction),
				fmt.Sprintf("%d : %s", rep.StoreId,
					kvapi.BytesHumanDisplay(statusUsed*(1<<20))),
				fmt.Sprintf("%d", statusVer),
			}...), attrs...))
		}
	}

	table.Render()
	msg := tbuf.String()

	msg += fmt.Sprintf("\n  Shards %d\n", len(dbMap.Shards))

	return msg, nil
}

type dbTestData struct{}

func (dbTestData) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "db-test-data",
		Desc: "db-test-data <database name> [-num N]",
	}
}

func (dbTestData) Action(fg flagSet, l *readline.Instance) (string, error) {

	var dbName string

	if len(fg.varArgs) > 0 {
		dbName = fg.varArgs[0]
	} else {

		l.SetPrompt("database name: ")
		dbName, err = l.Readline()
		if err != nil {
			return "", err
		}
	}

	var (
		tn  = time.Now()
		num = 10000
		siz int64
	)

	if fg.Value("num").Int64() > 0 {
		num = int(fg.Value("num").Int64())
		if num > 1000000 {
			num = 1000000
		}
	}

	for i := 0; i < num; i++ {

		var (
			key = []byte(fmt.Sprintf("_magic_test_key_%s", server.RandHexString(12)))
			val = server.RandBytes(4 << 10)
		)

		wr := kvapi.NewWriteRequest(key, val)
		wr.Database = dbName

		rs := kvclient.Write(wr)
		if rs.OK() {
			fmt.Printf("#%08d write key %s ok\n", i, string(key))
		} else {
			fmt.Printf("write key %s fail %v\n", string(key), rs.ErrorMessage())
			break
		}

		siz += int64(len(key) + len(val))
	}

	lat := float64(time.Now().UnixNano()-tn.UnixNano()) / 1e9
	if lat < 0.1 {
		lat = 0.1
	}

	return fmt.Sprintf("write %d keys in %v, tps %d, %s",
		num, time.Since(tn),
		int(float64(num)/float64(lat)), kvapi.BytesHumanDisplay(siz)), nil
}
