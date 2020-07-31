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

	"github.com/hooto/hauth/go/hauth/v1"
	"google.golang.org/grpc/credentials"
)

const (
	authKeyAccessKeySystem = "00000000"
)

func authKeyDefault() *hauth.AuthKey {
	return &hauth.AuthKey{
		AccessKey: authKeyAccessKeySystem,
		SecretKey: "<empty>",
	}
}

func newAppCredential(key *hauth.AuthKey) credentials.PerRPCCredentials {
	return hauth.NewGrpcAppCredential(key)
}

func appAuthParse(ctx context.Context) (*hauth.AppValidator, error) {
	return hauth.GrpcAppValidator(ctx)
}

func appAuthValid(ctx context.Context, keyMgr *hauth.AuthKeyManager) error {
	return hauth.GrpcAppCredentialValid(ctx, keyMgr)
}
