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
	"bytes"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	mrand "math/rand"
	"net"
	"regexp"
	"time"

	"github.com/cespare/xxhash"

	kv2 "github.com/lynkdb/kvspec/v2/go/kvspec"
)

var (
	hex16 = regexp.MustCompile("^[a-f0-9]{16}$")
)

func debugPrint(args ...interface{}) {
	fmt.Println(args...)
}

func keyExpireEncode(ns byte, expired uint64, key []byte) []byte {
	return append(append([]byte{ns}, uint64ToBytes(expired)...), key...)
}

func keyEncode(ns byte, key []byte) []byte {
	return append([]byte{ns}, key...)
}

func BytesClone(src []byte) []byte {
	return bytesClone(src)
}

func bytesClone(src []byte) []byte {

	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

func uint32ToBytes(v uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, v)
	return bs
}

func uint32ToHexString(v uint32) string {
	return bytesToHexString(uint32ToBytes(v))
}

func uint64ToBytes(v uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)
	return bs
}

func uint64ToHexString(v uint64) string {
	return bytesToHexString(uint64ToBytes(v))
}

func bytesToHexString(bs []byte) string {
	return hex.EncodeToString(bs)
}

func randHexString(length int) string {

	length = length / 2
	if length < 1 {
		length = 1
	}
	if n := length % 2; n > 0 {
		length += n
	}

	bs := make([]byte, length)
	if _, err := crand.Read(bs); err != nil {
		for i := range bs {
			bs[i] = uint8(mrand.Intn(256))
		}
	}

	return hex.EncodeToString(bs)
}

func TLSCertCreate(cn string) (*ConfigTLSCertificate, error) {

	tn := time.Now()

	crt := &x509.Certificate{
		SerialNumber: big.NewInt(mrand.Int63()),
		Subject: pkix.Name{
			Country:            []string{""},
			Organization:       []string{"lynkdb"},
			OrganizationalUnit: []string{"kvgo"},
			CommonName:         cn,
		},
		NotBefore:             tn,
		NotAfter:              tn.AddDate(10, 0, 0), // 10 years
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
	}

	key, _ := rsa.GenerateKey(crand.Reader, 2048)

	buf, err := x509.CreateCertificate(crand.Reader, crt, crt, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	return &ConfigTLSCertificate{
		ServerCertData: pemEncode("CERTIFICATE", buf),
		ServerKeyData:  pemEncode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)),
	}, nil
}

func pemEncode(name string, bs []byte) string {

	var (
		buf   bytes.Buffer
		block = &pem.Block{
			Bytes: bs,
			Type:  name,
		}
	)

	pem.Encode(&buf, block)

	return string(buf.Bytes())
}

func resultDataSize(rs *kv2.ObjectResult) int64 {
	siz := 0
	for _, item := range rs.Items {
		if item.Data != nil {
			siz += len(item.Data.Value)
		}
	}
	return int64(siz)
}

func batchResultDataSize(rs *kv2.BatchResult) int64 {
	siz := int64(0)
	for _, item := range rs.Items {
		siz += resultDataSize(item)
	}
	return siz
}

func timeNow() time.Time {
	return time.Now()
}

func timeus() int64 {
	return (time.Now().UnixNano() / 1e3)
}

func timems() int64 {
	return (time.Now().UnixNano() / 1e6)
}

func jsonEncode(obj interface{}) []byte {
	bs, _ := json.Marshal(obj)
	return bs
}

func jsonDecode(bs []byte, obj interface{}) error {
	return json.Unmarshal(bs, obj)
}

func jsonPrint(name string, obj interface{}) {
	log.Printf("%s %s", name, string(jsonEncode(obj)))
}

func privateIP4Valid(ipAddr string) error {

	// Private IPv4
	// 10.0.0.0 ~ 10.255.255.255
	// 172.16.0.0 ~ 172.31.255.255
	// 192.168.0.0 ~ 192.168.255.255

	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return errors.New("invalid ip address")
	}

	ip = ip.To4()

	ipa := int(ip[0])
	ipb := int(ip[1])

	if ipa == 10 ||
		(ipa == 172 && ipb >= 16 && ipb <= 31) ||
		(ipa == 192 && ipb == 168) {
		return nil
	}

	return errors.New("invalid private ip address")
}

func hash64(b []byte) uint64 {
	return xxhash.Sum64(b)
}
