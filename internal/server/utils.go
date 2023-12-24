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

	"github.com/lynkdb/kvgo/v2/pkg/kvapi"
)

var (
	hex16 = regexp.MustCompile("^[a-f0-9]{16}$")

	testLogOutput = true
)

func init() {
	mrand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

func debugPrint(args ...interface{}) {
	fmt.Println(args...)
}

func BytesClone(src []byte) []byte {
	return bytesClone(src)
}

func bytesClone(src []byte) []byte {

	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

func objectClone(src, dst interface{}) error {
	js, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(js, dst)
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

func randUint64() uint64 {
	return mrand.Uint64()
}

func randFloat64(v float64) float64 {
	if v < 0.0001 {
		v = 0.0001
	}
	return v * mrand.Float64()
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

func randBytes(size int) []byte {

	if size < 1 {
		size = 1
	} else if size > 2000000 {
		size = 2000000
	}

	bs := make([]byte, size)

	if _, err := crand.Read(bs); err != nil {
		for i := range bs {
			bs[i] = uint8(mrand.Intn(256))
		}
	}

	return bs
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

func resultDataSize(rs *kvapi.ResultSet) int64 {
	siz := 0
	for _, item := range rs.Items {
		siz += len(item.Key)
		siz += len(item.Value)
	}
	return int64(siz)
}

func batchResultDataSize(rs *kvapi.BatchResponse) int64 {
	siz := int64(0)
	for _, item := range rs.Items {
		siz += resultDataSize(item)
	}
	return siz
}

func timeNow() time.Time {
	return time.Now()
}

func timens() int64 {
	return (time.Now().UnixNano())
}

func timeus() int64 {
	return (time.Now().UnixNano() / 1e3)
}

func timems() int64 {
	return (time.Now().UnixNano() / 1e6)
}

func timesec() int64 {
	return (time.Now().Unix())
}

func jsonEncode(obj interface{}) []byte {
	bs, _ := json.Marshal(obj)
	return bs
}

func jsonDecode(bs []byte, obj interface{}) error {
	return json.Unmarshal(bs, obj)
}

func jsonPrint(name string, obj interface{}) {
	// bs, _ := json.MarshalIndent(obj, "", "  ")
	bs, _ := json.Marshal(obj)
	// log.Printf("%s %s", name, string(bs))
	log.Output(2, name+" "+string(bs))
}

func testPrintf(fmts string, args ...interface{}) {
	if testLogOutput {
		log.Output(2, fmt.Sprintf(fmts, args...))
	}
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

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func absFloat64(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func bytesRepeat(b []byte, c byte, siz int) []byte {
	if n := siz - len(b); n > 0 {
		b = append(b, bytes.Repeat([]byte{c}, n)...)
	}
	return b
}

func middleKey(lower, upper []byte) ([]byte, bool) {
	if bytes.Compare(lower, upper) >= 0 {
		return lower, false
	}
	if n := len(lower) - len(upper); n > 0 {
		upper = append(upper, make([]byte, n)...)
	}
	var (
		mid = []byte{}
	)
	for i := 0; i < len(lower); i++ {
		if lower[i] == upper[i] {
			mid = append(mid, lower[i])
		} else if lower[i] < upper[i] {
			if lower[i]+2 < upper[i] {
				mid = append(mid, lower[i]+((upper[i]-lower[i])/2))
			} else if lower[i]+1 == upper[i] {
				if i+1 < len(lower) {
					if i+1 >= len(upper) {
						upper = append(upper, 0x00)
					}
					if lower[i+1] == 0xff {
						mid = append(mid, upper[i])
						mid = append(mid, 0x00)
					} else {
						ln := (binary.BigEndian.Uint16(upper[i:i+2]) -
							binary.BigEndian.Uint16(lower[i:i+2])) / 2
						ln += binary.BigEndian.Uint16(lower[i : i+2])
						mid = append(mid, []byte{0x00, 0x00}...)
						binary.BigEndian.PutUint16(mid[i:i+2], ln)
					}
				} else {
					mid = append(mid, lower[i])
					mid = append(mid, 0x80)
				}
			} else {
				mid = append(mid, lower[i]+1)
			}
			return mid, true
		} else {
			return mid, false
		}
	}
	if bytes.Compare(mid, lower[:len(mid)]) == 0 {
		upper = append(upper, 0xff)
		for i := len(mid); i < len(upper); i++ {
			if upper[i] == 0x00 {
				mid = append(mid, 0x00)
			} else {
				if upper[i] > 0x01 {
					mid = append(mid, upper[i]/2)
				} else {
					mid = append(mid, 0x00)
				}
				return mid, true
			}
		}
	}
	return mid, true
}
