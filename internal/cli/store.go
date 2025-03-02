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
	"time"

	"github.com/lynkdb/lynkapi/go/lynkapi"
	"github.com/lynkdb/lynkapi/go/lynkcli"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	lynkcli.RegisterRender("admin", "store-info", StoreInfoRender)
}

var StoreInfoRender = func(data *structpb.Struct) (string, error) {
	var rs server.StoreInfoResponse
	if err := lynkapi.DecodeStruct(data, &rs); err != nil {
		return "", err
	}

	if len(rs.Items) == 0 {
		return "", fmt.Errorf("no response")
	}

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewWriter(&tbuf)

		sumUsed int64
		sumFree int64
	)

	table.SetHeader([]string{
		"ID", "UID", "Reps",
		"Free", "Used/Percent", "Block (Data,Meta,Log,TTL)", "Options",
		"Updated"})

	table.SetCenterSeparator("|")
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetRowLine(true)
	table.SetAutoWrapText(false)

	for _, item := range rs.Items {

		var (
			rate string
			all  = float64(item.CapacityUsed + item.CapacityFree)
			opts = []string{}
		)

		if all == 0 {
			rate = "0"
		} else {
			rate = fmt.Sprintf("%.2f %%", float64(item.CapacityUsed)*100/all)
		}

		if len(item.Options) > 0 {
			for k, v := range item.Options {
				opts = append(opts, k+" : "+v)
			}
			sort.Slice(opts, func(i, j int) bool {
				return strings.Compare(opts[i], opts[j]) < 0
			})
		}

		sumUsed += item.CapacityUsed
		sumFree += item.CapacityFree

		table.Append([]string{
			fmt.Sprintf("%d", item.Id),
			item.UniId,
			fmt.Sprintf("%d", item.ReplicaBounds),
			kvapi.BytesHumanDisplay(item.CapacityFree * kvapi.MiB),
			kvapi.BytesHumanDisplay(item.CapacityUsed*kvapi.MiB) + "/" + rate,
			fmt.Sprintf("%d %s, %d %s, %d %s, %d %s",
				item.DataKeys,
				kvapi.BytesHumanDisplay(item.DataUsed*kvapi.MiB),
				item.MetaKeys,
				kvapi.BytesHumanDisplay(item.MetaUsed*kvapi.MiB),
				item.LogKeys,
				kvapi.BytesHumanDisplay(item.LogUsed*kvapi.MiB),
				item.TtlKeys,
				kvapi.BytesHumanDisplay(item.TtlUsed*kvapi.MiB),
			),
			strings.Join(opts, ", "),
			time.Unix(item.Updated, 0).Format(time.DateTime),
		})
	}

	table.SetFooter([]string{"", "", "",
		kvapi.BytesHumanDisplay(sumFree * kvapi.MiB),
		kvapi.BytesHumanDisplay(sumUsed*kvapi.MiB) + "/" +
			fmt.Sprintf("%.2f %%", float64(sumUsed)*100/float64(sumUsed+sumFree+1)),
		"", ""})

	table.Render()

	return tbuf.String(), nil
}
