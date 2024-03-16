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

package kvrep

import (
	"github.com/lynkdb/kvgo/v2/internal/server"
	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
	"github.com/lynkdb/kvgo/v2/pkg/storage"
	_ "github.com/lynkdb/kvgo/v2/pkg/storage/pebble"
)

func NewReplica(opts *storage.Options) (kvapi.Client, error) {

	db, err := storage.Open(storage.DefaultDriver, opts)
	if err != nil {
		return nil, err
	}

	scfg := &server.Config{}
	scfg.Feature.WriteLogDisable = true

	c, err := server.NewDatabase(db, "0000", "local-replica", 1, 2, scfg)
	if err != nil {
		return nil, err
	}

	return c, nil
}
