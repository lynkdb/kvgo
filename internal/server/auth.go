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

	"github.com/sysinner/innerstack/v2/pkg/inauth"
	"google.golang.org/grpc/credentials"
)

const (
	authKeyAccessKeySystem   = "00000000"
	authKeyAccessKeyClient01 = "00000001"
)

func authKeyDefault() *inauth.AccessKey {
	return &inauth.AccessKey{
		Id:     authKeyAccessKeySystem,
		Secret: "<empty>",
	}
}

func NewSystemAccessKey() *inauth.AccessKey {
	key := inauth.NewAccessKey()
	key.Id = authKeyAccessKeySystem
	key.Roles = []string{"sa"}
	key.Scopes = []string{"*"}
	return key
}

func newAppCredential(key *inauth.AccessKey) credentials.PerRPCCredentials {
	return inauth.NewGrpcAppCredential(key)
}

func appAuthParse(ctx context.Context, keyMgr *inauth.AccessKeyManager) (inauth.AppValidator, error) {
	return inauth.NewGrpcAppValidator(ctx, keyMgr)
}
