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

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

func init() {
	register(new(cmdStoreInfo))
}

type cmdStoreInfo struct{}

func (cmdStoreInfo) Spec() baseCommandSpec {
	return baseCommandSpec{
		Path: "store info",
	}
}

func (cmdStoreInfo) Action(fg flagSet, l *readline.Instance) (string, error) {

	req := &kvapi.SysGetRequest{
		Name: "store-info",
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

	type storeStatusManager struct {
		Items []*kvapi.SysStoreStatus `json:"items"`
	}

	var (
		tbuf  bytes.Buffer
		table = tablewriter.NewWriter(&tbuf)

		status storeStatusManager

		sumUsed int64
		sumFree int64
	)

	table.SetHeader([]string{
		"ID", "Uni ID", "Rep Bounds",
		"Free", "Used", "Used Percent", "Options",
		"Updated"})

	table.SetCenterSeparator("|")
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetRowLine(true)
	table.SetAutoWrapText(false)

	if err := rs.Lookup([]byte("store/status")).JsonDecode(&status); err != nil {
		return "", err
	}

	for _, item := range status.Items {

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
			kvapi.BytesHumanDisplay(item.CapacityUsed * kvapi.MiB),
			rate,
			strings.Join(opts, ", "),
			time.Unix(item.Updated, 0).Format(time.DateTime),
		})
	}

	table.SetFooter([]string{"", "", "",
		kvapi.BytesHumanDisplay(sumFree * kvapi.MiB),
		kvapi.BytesHumanDisplay(sumUsed * kvapi.MiB),
		fmt.Sprintf("%.2f %%", float64(sumUsed)*100/float64(sumUsed+sumFree+1)),
		"", ""})

	table.Render()

	return tbuf.String(), nil
}
