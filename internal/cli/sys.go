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
	"encoding/json"
	"fmt"

	// "github.com/olekukonko/tablewriter"
	"github.com/chzyer/readline"

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

func Status(l *readline.Instance) (string, error) {

	req := &kvapi.StatusRequest{}

	rs := adminClient.Status(req)
	if !rs.OK() {
		return "", rs.Error()
	}

	js, _ := json.MarshalIndent(rs, "", "  ")
	fmt.Println(string(js))

	return "", nil
}

/**
func SysStatus() (string, error) {
	req := kv2.NewSysCmdRequest("SysStatus", nil)

	rs := data.Connector().SysCmd(req)
	if !rs.OK() {
		return "", rs.Error()
	}

	if len(rs.Items) == 0 {
		return "", fmt.Errorf("no data found")
	}

	var item kv2.SysStatus
	if err := rs.Items[0].DataValue().Decode(&item, nil); err != nil {
		return "", err
	}

	var buf bytes.Buffer

	if len(item.Nodes) > 0 {

		fmt.Fprintf(&buf, "%s\n", "Nodes")

		table := tablewriter.NewWriter(&buf)
		table.SetHeader([]string{"ID", "Addr", "Version", "Uptime", "CPU", "RAM", "DISK"})

		var (
			cpu  int64
			ram  int64
			disk int64
		)

		for _, v := range item.Nodes {
			cols := []string{
				v.Id,
				v.Addr,
				v.Version,
				uptimeFormat(time.Now().Unix() - v.Uptime),
			}
			if len(v.Caps) > 0 {

				if c, ok := v.Caps["cpu"]; ok {
					cols = append(cols, fmt.Sprintf("%d", c.Use))
					cpu += c.Use
				} else {
					cols = append(cols, " ")
				}

				if c, ok := v.Caps["mem"]; ok && c.Use > 0 {
					cols = append(cols, sizeFormat(c.Use))
					ram += c.Use
				} else {
					cols = append(cols, " ")
				}

				if c, ok := v.Caps["disk"]; ok && c.Use > 0 {
					cols = append(cols, sizeFormat(c.Use))
					disk += c.Use
				} else {
					cols = append(cols, " ")
				}
			} else {
				cols = append(cols, []string{" ", " ", " "}...)
			}

			table.Append(cols)
		}

		if len(item.Nodes) > 1 {
			table.SetFooter([]string{" ", " ", " ", " ", fmt.Sprintf("%d", cpu), sizeFormat(ram), sizeFormat(disk)})
		}

		table.Render()
		buf.WriteString("\n")
	}

	if len(item.Tables) > 0 {

		fmt.Fprintf(&buf, "%s\n", "Tables")

		table := tablewriter.NewWriter(&buf)
		table.SetHeader([]string{"Name", "Size", "Status"})
		table.SetRowLine(true)
		table.SetAutoWrapText(false)

		size := int64(0)

		rx := regexp.MustCompile("^size\\:(\\d+)$")

		trySizeParse := func(k, v string) string {
			if hit := rx.FindStringSubmatch(v); len(hit) == 2 {
				if i, e := strconv.ParseInt(hit[1], 10, 64); e == nil && i >= 0 {
					return fmt.Sprintf("%s: %s", k, sizeFormat(i))
				}
			}
			return k + ": " + v
		}

		for _, v := range item.Tables {
			cols := []string{
				v.Name,
				sizeFormat(int64(v.DbSize)),
			}
			size += int64(v.DbSize)
			if len(v.States) > 0 {
				ar := []string{}
				for k2, v2 := range v.States {
					ar = append(ar, trySizeParse(k2, v2))
				}
				sort.Slice(ar, func(i, j int) bool {
					return strings.Compare(ar[i], ar[j]) < 0
				})
				cols = append(cols, strings.Join(ar, "\n"))
			} else {
				cols = append(cols, "")
			}

			table.Append(cols)
		}

		if len(item.Tables) > 1 {
			table.SetFooter([]string{" ", sizeFormat(size), " "})
		}

		table.Render()
		buf.WriteString("\n")
	}

	if len(item.Volumes) > 0 {

		fmt.Fprintf(&buf, "%s\n", "Volumes")

		table := tablewriter.NewWriter(&buf)
		table.SetHeader([]string{"Id", "Mountpoint"})
		table.SetRowLine(true)

		for _, v := range item.Volumes {
			cols := []string{
				v.Id,
				v.Mountpoint,
			}
			table.Append(cols)
		}

		table.Render()
		buf.WriteString("\n")
	}

	return buf.String(), nil
}
*/
