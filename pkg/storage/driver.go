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

package storage

import (
	"errors"
	"fmt"
	"sync"
)

const (
	DriverV2      = "v2"
	DefaultDriver = "v2"
)

type Driver interface {
	// Driver specific name.
	Name() string

	// Open returns a new connection to the database.
	Open(opts *Options) (Conn, error)
}

var (
	dmu     sync.Mutex
	drivers = map[string]Driver{}
)

func Register(drv Driver) {
	if drv == nil {
		return
	}
	dmu.Lock()
	defer dmu.Unlock()
	if _, ok := drivers[drv.Name()]; !ok {
		drivers[drv.Name()] = drv
	}
}

func Open(driver string, opts *Options) (Conn, error) {

	dmu.Lock()
	drv, ok := drivers[driver]
	dmu.Unlock()
	if !ok {
		return nil, fmt.Errorf("driver (%s) not found", driver)
	}

	if opts == nil {
		return nil, errors.New("storage.Options not set")
	}
	if err := opts.Valid(); err != nil {
		return nil, err
	}
	opts.Reset()

	return drv.Open(opts)
}
