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
	"time"

	"github.com/chzyer/readline"
	"github.com/lynkdb/lynkapi/go/lynkapi"
	"github.com/lynkdb/lynkapi/go/lynkcli"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	lynkcli.RegisterCommonCommand(new(dbTestData))

	lynkcli.RegisterRender("admin", "database-info", DatabaseInfoRender)
	// lynkcli.RegisterRender("admin", "database-key-stats", DatabaseKeyStatsRender)
}

var DatabaseInfoRender = func(data *structpb.Struct) (string, error) {
	var rs server.DatabaseInfo
	if err := lynkapi.DecodeStruct(data, &rs); err != nil {
		return "", err
	}

	if rs.Map == nil || rs.Status == nil {
		return "", fmt.Errorf("no response")
	}

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewTable(&tbuf)

		dbMap    = rs.Map
		dbStatus = rs.Status
	)

	table.Options(tablewriter.WithRendition(tw.Rendition{
		Settings: tw.Settings{
			Separators: tw.Separators{BetweenRows: tw.On},
		},
	}))

	table.Header([]string{
		"Shard", "Action", "Ver", "Updated",
		"Rep ID", "Rep Action", "Rep Store", "Log",
		"Offset"})

	// table.SetAutoMergeCellsByColumnIndex([]int{0, 1, 2, 3, 7})
	// table.SetCenterSeparator("|")
	// table.SetAlignment(tablewriter.ALIGN_LEFT)
	// table.SetRowLine(true)
	// table.SetAutoWrapText(false)

	if dbStatus.Replicas == nil {
		dbStatus.Replicas = map[uint64]*kvapi.DatabaseMapStatus_Replica{}
	}

	for _, shard := range dbMap.Shards {

		var idflow string
		if shard.Prev > 0 {
			idflow = fmt.Sprintf("%d -> %d", shard.Prev, shard.Id)
		} else {
			idflow = fmt.Sprintf("%d", shard.Id)
		}

		cols := []string{
			idflow,
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
				statusKeys   int64
				statusUsed   int64
				statusLog    uint64
			)

			if repStatus, ok := dbStatus.Replicas[rep.Id]; ok {
				statusAction = repStatus.Action
				statusKeys = repStatus.Keys
				statusUsed = repStatus.Used
				statusLog = repStatus.LogVersion
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
				fmt.Sprintf("%d : %d %s", rep.StoreId,
					statusKeys,
					kvapi.BytesHumanDisplay(statusUsed*(1<<20))),
				fmt.Sprintf("%d", statusLog),
			}...), attrs...))
		}
	}

	table.Render()
	msg := "\n" + tbuf.String()

	msg += fmt.Sprintf(" Shards %d\n", len(dbMap.Shards))

	if len(rs.KeyStats) > 0 {
		if msg2, err := databaseInfoKeyStatsRender(rs); err == nil {
			msg += msg2
		}
	}

	return msg, nil
}

var databaseInfoKeyStatsRender = func(rs server.DatabaseInfo) (string, error) {

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewWriter(&tbuf)
	)

	table.Options(tablewriter.WithRendition(tw.Rendition{
		Settings: tw.Settings{
			Separators: tw.Separators{BetweenRows: tw.On},
		},
	}))

	table.Header([]string{
		"Key", "Num Keys", "Size",
	})

	// table.SetCenterSeparator("|")
	// table.SetAlignment(tablewriter.ALIGN_LEFT)
	// table.SetRowLine(true)
	// table.SetAutoWrapText(false)

	num := int64(0)

	for _, ks := range rs.KeyStats {

		cols := []string{
			string(ks.Key),
			fmt.Sprintf("%d", ks.Num),
			kvapi.BytesHumanDisplay(ks.SizeMb * kvapi.MiB),
		}

		num += ks.Num

		table.Append(cols)
	}

	table.Render()
	msg := "\n" + tbuf.String()

	msg += fmt.Sprintf(" Keys %d\n", num)
	return msg, nil
}

type dbTestData struct{}

func (dbTestData) Spec() lynkcli.BaseCommandSpec {
	return lynkcli.BaseCommandSpec{
		Path: "db-test-data",
		Desc: "db-test-data <database name> [-num N]",
	}
}

func (dbTestData) Action(fg lynkcli.FlagSet, l *readline.Instance) (string, error) {

	var dbName string

	if len(fg.VarArgs) > 0 {
		dbName = fg.VarArgs[0]
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
