// Copyright 2015 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
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

package kvgo

import (
	"context"
	"time"

	"github.com/hooto/hauth/go/hauth/v1"
	"google.golang.org/grpc"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

type ClientConfig struct {
	Addr        string                `toml:"addr" json:"addr"`
	AuthKey     *hauth.AuthKey        `toml:"auth_key" json:"auth_key"`
	AuthTLSCert *ConfigTLSCertificate `toml:"auth_tls_cert" json:"auth_tls_cert"`
	Options     *kv2.ClientOptions    `toml:"options,omitempty" json:"options,omitempty"`
	// TODEL
	AuthSecretKeyDel string `toml:"auth_secret_key" json:"auth_secret_key"`
}

type ClientConnector struct {
	conn *grpc.ClientConn
}

func (it *ClientConfig) NewClient() (kv2.Client, error) {

	conn, err := clientConn(it.Addr, it.AuthKey, it.AuthTLSCert)
	if err != nil {
		return nil, err
	}

	c, err := kv2.NewClient(&ClientConnector{
		conn: conn,
	})

	if err != nil {
		return nil, err
	}

	if it.Options != nil {
		c.OptionApply(it.Options)
	}

	return c, nil
}

/**
func (it *ClientConnector) newContext() context.Context {

}
*/

func (it *ClientConnector) Query(req *kv2.ObjectReader) *kv2.ObjectResult {

	ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
	defer fc()

	rs, err := kv2.NewPublicClient(it.conn).Query(ctx, req)
	if err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	return rs
}

func (it *ClientConnector) Commit(req *kv2.ObjectWriter) *kv2.ObjectResult {

	ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
	defer fc()

	rs, err := kv2.NewPublicClient(it.conn).Commit(ctx, req)
	if err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	return rs
}

func (it *ClientConnector) BatchCommit(req *kv2.BatchRequest) *kv2.BatchResult {

	ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
	defer fc()

	rs, err := kv2.NewPublicClient(it.conn).BatchCommit(ctx, req)
	if err != nil {
		return req.NewResult(kv2.ResultClientError, err.Error())
	}

	return rs
}

func (it *ClientConnector) SysCmd(req *kv2.SysCmdRequest) *kv2.ObjectResult {

	ctx, fc := context.WithTimeout(context.Background(), time.Second*3)
	defer fc()

	rs, err := kv2.NewPublicClient(it.conn).SysCmd(ctx, req)
	if err != nil {
		return kv2.NewObjectResultClientError(err)
	}

	return rs
}

func (it *ClientConnector) Close() error {
	if it.conn != nil {
		return it.conn.Close()
	}
	return nil
}
