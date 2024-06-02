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

package kvapi

type ClientReader interface {
	SetMetaOnly(b bool) ClientReader
	SetAttrs(attrs uint64) ClientReader

	Exec() *ResultSet
}

type ClientRanger interface {
	SetLimit(v int64) ClientRanger
	SetRevert(b bool) ClientRanger

	Exec() *ResultSet
}

type ClientWriter interface {
	SetJsonValue(v interface{}) ClientWriter

	SetCreateOnly(b bool) ClientWriter
	SetTTL(ttl int64) ClientWriter
	SetAttrs(attrs uint64) ClientWriter
	SetIncr(id uint64, ns string) ClientWriter

	SetPrevVersion(v uint64) ClientWriter
	SetPrevChecksum(v interface{}) ClientWriter

	Exec() *ResultSet
}

type ClientDeleter interface {
	SetRetainMeta(b bool) ClientDeleter

	SetPrevVersion(v uint64) ClientDeleter
	SetPrevChecksum(v interface{}) ClientDeleter

	Exec() *ResultSet
}

type Client interface {
	Write(req *WriteRequest) *ResultSet
	Delete(req *DeleteRequest) *ResultSet
	Read(req *ReadRequest) *ResultSet
	Range(req *RangeRequest) *ResultSet
	Batch(req *BatchRequest) *BatchResponse

	NewReader(key []byte, keys ...[]byte) ClientReader
	NewRanger(lowerKey, upperKey []byte) ClientRanger
	NewWriter(key []byte, value interface{}) ClientWriter
	NewDeleter(key []byte) ClientDeleter

	SetDatabase(name string) Client

	Close() error
}

// type AdminClient interface {
// 	DatabaseList(req *DatabaseListRequest) *ResultSet
// 	DatabaseCreate(req *DatabaseCreateRequest) *ResultSet
// 	DatabaseUpdate(req *DatabaseUpdateRequest) *ResultSet
// 	// JobList(req *JobListRequest) *ResultSet
// 	SysGet(req *SysGetRequest) *ResultSet
// 	Close() error
// }

type DebugClient interface {
	RawRange(req *RangeRequest) ([]*RawKeyValue, error)
}

type ClientOption interface {
	Apply(*ClientOptions)
}

type ClientOptions struct {
	// timeout in milliseconds
	Timeout ClientTimeout `toml:"timeout" json:"timeout"`
}

func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{
		Timeout: 10000,
	}
}

func (it *ClientOptions) Apply(opts *ClientOptions) {
	if it.Timeout > 0 {
		it.Timeout.Apply(opts)
	}
}

type ClientTimeout int64

func (it ClientTimeout) Apply(opts *ClientOptions) {
	if it < 1e3 {
		it = 1e3
	} else if it > 60e3 {
		it = 60e3
	}
	opts.Timeout = it
}
