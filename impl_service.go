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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"sync"

	"github.com/hooto/hauth/go/hauth/v1"
	"github.com/hooto/hlog4g/hlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

var (
	grpcMsgByteMax  = 12 * int(kv2.MiB)
	grpcClientConns = map[string]*grpc.ClientConn{}
	grpcClientMu    sync.Mutex
)

func (cn *Conn) serviceStart() error {

	if nCap := len(cn.opts.Cluster.MainNodes); nCap > 0 {

		if nCap > kv2.ObjectClusterNodeMax {
			return errors.New("Deny of kv2.ObjectClusterNodeMax")
		}

		var (
			masters = []*ClientConfig{}
			addrs   = map[string]bool{}
		)

		for _, v := range cn.opts.Cluster.MainNodes {

			host, port, err := net.SplitHostPort(v.Addr)
			if err != nil {
				return err
			}

			if _, ok := addrs[host+":"+port]; ok {
				hlog.Printf("warn", "Duplicate host:port (%s:%s) setting", host, port)
				continue
			}

			v.Addr = host + ":" + port

			if err := cn.keyMgr.KeySet(v.AuthKey); err != nil {
				return err
			}

			masters = append(masters, v)
		}

		cn.opts.Cluster.MainNodes = masters
	}

	if cn.opts.Server.Bind != "" && !cn.opts.ClientConnectEnable {

		if cn.opts.Server.AuthKey == nil {
			return errors.New("no [server.auth_key] setup")
		}

		cn.keyMgr.KeySet(cn.opts.Server.AuthKey)

		host, port, err := net.SplitHostPort(cn.opts.Server.Bind)
		if err != nil {
			return err
		}

		lis, err := net.Listen("tcp", ":"+port)
		if err != nil {
			return err
		}

		cn.opts.Server.Bind = host + ":" + port

		serverOptions := []grpc.ServerOption{
			grpc.MaxMsgSize(grpcMsgByteMax),
			grpc.MaxSendMsgSize(grpcMsgByteMax),
			grpc.MaxRecvMsgSize(grpcMsgByteMax),
		}

		if cn.opts.Server.AuthTLSCert != nil {

			cert, err := tls.X509KeyPair(
				[]byte(cn.opts.Server.AuthTLSCert.ServerCertData),
				[]byte(cn.opts.Server.AuthTLSCert.ServerKeyData))
			if err != nil {
				return err
			}

			certs := credentials.NewServerTLSFromCert(&cert)

			serverOptions = append(serverOptions, grpc.Creds(certs))
		}

		server := grpc.NewServer(serverOptions...)

		go server.Serve(lis)

		cn.public = &PublicServiceImpl{
			sock:     lis,
			server:   server,
			db:       cn,
			prepares: map[string]*kv2.ObjectWriter{},
		}

		cn.internal = &InternalServiceImpl{
			sock:     lis,
			server:   server,
			db:       cn,
			prepares: map[string]*kv2.ObjectWriter{},
		}

		kv2.RegisterPublicServer(server, cn.public)
		kv2.RegisterInternalServer(server, cn.internal)

	} else {
		cn.public = &PublicServiceImpl{
			db: cn,
		}
	}

	return nil
}

func clientConn(addr string, key *hauth.AuthKey, cert *ConfigTLSCertificate) (*grpc.ClientConn, error) {

	if key == nil {
		return nil, errors.New("not auth key setup")
	}

	grpcClientMu.Lock()
	defer grpcClientMu.Unlock()

	if c, ok := grpcClientConns[addr]; ok {
		return c, nil
	}

	dialOptions := []grpc.DialOption{
		grpc.WithPerRPCCredentials(newAppCredential(key)),
		grpc.WithMaxMsgSize(grpcMsgByteMax),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMsgByteMax)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(grpcMsgByteMax)),
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

	grpcClientConns[addr] = c

	return c, nil
}
