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

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hooto/hauth/go/hauth/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/lynkdb/kvgo/pkg/kvapi"
)

var (
	rpcClientConns = map[string]*grpc.ClientConn{}
	rpcClientMu    sync.Mutex
)

type ClientConfig struct {
	Addr        string                `toml:"addr" json:"addr"`
	AccessKey   *hauth.AccessKey      `toml:"access_key" json:"access_key"`
	AuthTLSCert *ConfigTLSCertificate `toml:"auth_tls_cert" json:"auth_tls_cert"`
	Options     *kvapi.ClientOptions  `toml:"options,omitempty" json:"options,omitempty"`

	mu sync.Mutex        `toml:"-" json:"-"`
	c  kvapi.Client      `toml:"-" json:"-"`
	ac kvapi.AdminClient `toml:"-" json:"-"`
}

type clientConn struct {
	cfg     *ClientConfig
	rpcConn *grpc.ClientConn
	table   string
	c       kvapi.KvgoClient
	err     error
}

type adminClientConn struct {
	cfg     *ClientConfig
	rpcConn *grpc.ClientConn
	ac      kvapi.KvgoAdminClient
	err     error
}

func (it *ClientConfig) NewClient() (kvapi.Client, error) {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.c == nil {

		conn, err := rpcClientConnect(it.Addr, it.AccessKey, it.AuthTLSCert, false)
		if err != nil {
			return nil, err
		}

		if it.Options == nil {
			it.Options = kvapi.DefaultClientOptions()
		}

		it.c = &clientConn{
			cfg:     it,
			rpcConn: conn,
			c:       kvapi.NewKvgoClient(conn),
		}
	}

	return it.c, nil
}

func (it *ClientConfig) NewAdminClient() (kvapi.AdminClient, error) {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.ac == nil {

		conn, err := rpcClientConnect(it.Addr, it.AccessKey, it.AuthTLSCert, false)
		if err != nil {
			return nil, err
		}

		it.ac = &adminClientConn{
			cfg:     it,
			rpcConn: conn,
			ac:      kvapi.NewKvgoAdminClient(conn),
		}
	}

	return it.ac, nil
}

func (it *ClientConfig) timeout() time.Duration {
	if it.Options == nil {
		it.Options = kvapi.DefaultClientOptions()
	}
	return time.Millisecond * time.Duration(it.Options.Timeout)
}

// func (it *clientConn) tryConnect(retry bool) error {
// 	if it.rpcConn == nil {
// 		conn, err := rpcClientConnect(it.cfg.Addr, it.cfg.AccessKey, it.cfg.AuthTLSCert, true)
// 		if err != nil {
// 			return err
// 		}
// 		it.rpcConn = conn
// 		it.c = kvapi.NewKvgoClient(conn)
// 	}
// 	return nil
// }

func (it *clientConn) Read(req *kvapi.ReadRequest) *kvapi.ResultSet {

	// if err := it.tryConnect(false); err != nil {
	// 	return newResultSetWithClientError(err.Error())
	// }

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Table == "" {
		req.Table = it.table
	}

	rs, err := it.c.Read(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}

	return rs
}

func (it *clientConn) Range(req *kvapi.RangeRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Table == "" {
		req.Table = it.table
	}

	rs, err := it.c.Range(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}

	return rs
}

func (it *clientConn) Write(req *kvapi.WriteRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Table == "" {
		req.Table = it.table
	}

	rs, err := it.c.Write(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}

	return rs
}

func (it *clientConn) Delete(req *kvapi.DeleteRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	if req.Table == "" {
		req.Table = it.table
	}

	rs, err := it.c.Delete(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}

	return rs
}

func (it *clientConn) Batch(req *kvapi.BatchRequest) *kvapi.BatchResponse {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	rs, err := it.c.Batch(ctx, req)
	if err != nil {
		return &kvapi.BatchResponse{
			StatusCode:    kvapi.Status_RequestTimeout,
			StatusMessage: err.Error(),
		}
	}

	return rs
}

func (it *clientConn) SetTable(name string) kvapi.Client {
	it.table = name
	return it
}

func (it *clientConn) Close() error {
	if it.rpcConn != nil {
		return it.rpcConn.Close()
	}
	return nil
}

func (it *adminClientConn) TableList(req *kvapi.TableListRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	rs, err := it.ac.TableList(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}
	return rs
}

func (it *adminClientConn) TableCreate(req *kvapi.TableCreateRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	rs, err := it.ac.TableCreate(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}
	return rs
}

func (it *adminClientConn) TableAlter(req *kvapi.TableAlterRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	rs, err := it.ac.TableAlter(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}
	return rs
}

func (it *adminClientConn) Status(req *kvapi.StatusRequest) *kvapi.ResultSet {

	ctx, fc := context.WithTimeout(context.Background(), it.cfg.timeout())
	defer fc()

	rs, err := it.ac.Status(ctx, req)
	if err != nil {
		return newResultSetWithClientError(err.Error())
	}
	return rs
}

func (it *adminClientConn) Close() error {
	if it.rpcConn != nil {
		return it.rpcConn.Close()
	}
	return nil
}

func rpcClientConnect(addr string,
	key *hauth.AccessKey, cert *ConfigTLSCertificate,
	forceNew bool) (*grpc.ClientConn, error) {

	if key == nil {
		return nil, errors.New("not auth key setup")
	}

	ck := fmt.Sprintf("%s.%s", addr, key.Id)

	rpcClientMu.Lock()
	defer rpcClientMu.Unlock()

	if c, ok := rpcClientConns[ck]; ok {
		if forceNew {
			c.Close()
			c = nil
			delete(rpcClientConns, ck)
		} else {
			return c, nil
		}
	}

	dialOptions := []grpc.DialOption{
		grpc.WithPerRPCCredentials(newAppCredential(key)),
		grpc.WithMaxMsgSize(grpcMsgByteMax * 2),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMsgByteMax * 2)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMsgByteMax * 2)),
	}

	if cert == nil {

		dialOptions = append(dialOptions, grpc.WithInsecure())

	} else {

		block, _ := pem.Decode([]byte(cert.ServerCertData))
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("failed to decode CERTIFICATE")
		}

		crt, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, errors.New("failed to parse cert : " + err.Error())
		}

		certPool := x509.NewCertPool()
		certPool.AddCert(crt)

		// creds := credentials.NewClientTLSFromCert(certPool, addr)
		creds := credentials.NewTLS(&tls.Config{
			ServerName: crt.Subject.CommonName,
			RootCAs:    certPool,
		})

		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	}

	c, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		return nil, err
	}

	rpcClientConns[ck] = c

	return c, nil
}
