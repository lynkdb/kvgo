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
	"fmt"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

func sizeFormat(siz int64) string {

	sizeS := "0"
	if siz > kv2.TiB {
		sizeS = fmt.Sprintf("%.2f TB", float64(siz)/float64(kv2.TiB))
	} else if siz > kv2.GiB {
		sizeS = fmt.Sprintf("%.2f GB", float64(siz)/float64(kv2.GiB))
	} else if siz > kv2.MiB {
		sizeS = fmt.Sprintf("%d MB", siz/kv2.MiB)
	} else if siz > kv2.KiB {
		sizeS = fmt.Sprintf("%d KB", siz/kv2.KiB)
	}
	return sizeS
}

func uptimeFormat(sec int64) string {

	s := ""

	d := (sec / 86400)
	if d > 1 {
		s = fmt.Sprintf("%d days ", d)
	} else if d == 1 {
		s = fmt.Sprintf("%d day ")
	}

	sec = sec % 86400
	h := sec / 3600

	sec = sec % 3600
	m := sec / 60

	sec = sec % 60

	s += fmt.Sprintf("%02d:%02d:%02d", h, m, sec)

	return s
}
