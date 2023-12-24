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

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func NewWriteRequest(key, value []byte) *WriteRequest {
	req := &WriteRequest{
		Meta: &Meta{},
		Key:  key,
	}
	if len(value) > 0 {
		req.Value = value
		req.Meta.Checksum = bytesCrc32Checksum(value)
		req.Meta.Size = int32(len(value))
	}
	return req
}

func (it *WriteRequest) Valid() error {

	if it.Meta == nil {
		it.Meta = &Meta{}
	}

	if len(it.Key) < keyValue_MinKeyLen ||
		len(it.Key) > keyValue_MaxKeyLen {
		return fmt.Errorf("Invalid Key (len %d)", len(it.Key))
	}

	if AttrAllow(it.Attrs, Write_Attrs_IgnoreMeta) &&
		AttrAllow(it.Attrs, Write_Attrs_IgnoreData) {
		return errors.New("attrs conflict with ignore-meta and ignore-data")
	}

	if len(it.Meta.Extra) > keyValue_Meta_Extra_MaxLen {
		return fmt.Errorf("Invalid Meta/Extra (len %d)", len(it.Meta.Extra))
	}

	if it.Meta.IncrId > 0 && it.IncrNamespace == "" {
		it.IncrNamespace = "meta"
	}

	if it.IncrNamespace != "" &&
		!IncrNamespaceRX.MatchString(it.IncrNamespace) {
		return errors.New("Invalid IncrNamespace : " + it.IncrNamespace)
	}

	if len(it.Value) == 0 {
		return errors.New("Value Not Found")
	}

	return nil
}

func (it *WriteRequest) MetaEncode() ([]byte, error) {

	if len(it.Value) > 0 {
		it.Meta.Checksum = bytesCrc32Checksum(it.Value)
		it.Meta.Size = int32(len(it.Value))
	} else {
		it.Meta.Checksum = 0
		it.Meta.Size = 0
	}

	meta, err := StdProto.Encode(it.Meta)
	if err != nil {
		return nil, err
	}

	if len(meta) == 0 || len(meta) >= keyValue_Meta_MaxSize+100 {
		return nil, errors.New("invalid meta format")
	}

	if len(meta) < 256 {
		return append([]byte{keyValueMetaVersion1, uint8(len(meta))}, meta...), nil
	}

	b := []byte{keyValueMetaVersion2, 0, 0}
	binary.BigEndian.PutUint16(b[1:], uint16(len(meta)))
	return append(b, meta...), nil
}

func (it *WriteRequest) LogEncode(id uint64) ([]byte, error) {
	logMeta := &LogMeta{
		Id:      id,
		Version: it.Meta.Version,
		Attrs:   it.Meta.Attrs,
		Expired: it.Meta.Expired,
		Size:    it.Meta.Size,
		Key:     it.Key,
		Created: timems(),
	}
	return StdProto.Encode(logMeta)
}

func (it *WriteRequest) SetTTL(ttl int64) *WriteRequest {
	if ttl > 0 {
		it.Meta.Expired = timems() + ttl
	}
	return it
}

func (it *WriteRequest) SetAttrs(attrs uint64) *WriteRequest {
	it.Attrs |= attrs
	return it
}

func (it *WriteRequest) Encode() ([]byte, []byte, error) {

	meta, err := it.MetaEncode()
	if err != nil {
		return nil, nil, err
	}

	if len(it.Value) == 0 {
		return meta, meta, nil
	}

	return meta, append(meta, it.Value...), nil
}

func NewDeleteRequest(key []byte) *DeleteRequest {
	req := &DeleteRequest{
		Key: key,
	}
	return req
}

func (it *DeleteRequest) Valid() error {

	if len(it.Key) < keyValue_MinKeyLen ||
		len(it.Key) > keyValue_MaxKeyLen {
		return fmt.Errorf("Invalid Key (len %d)", len(it.Key))
	}

	return nil
}

func (it *DeleteRequest) SetRetainMeta(b bool) *DeleteRequest {
	if b {
		it.Attrs = AttrAppend(it.Attrs, Write_Attrs_RetainMeta)
	} else {
		it.Attrs = AttrRemove(it.Attrs, Write_Attrs_RetainMeta)
	}
	return it
}

func (it *DeleteRequest) LogEncode(id, ver uint64) ([]byte, error) {
	logMeta := &LogMeta{
		Id:      id,
		Version: ver,
		Attrs:   it.Attrs | Write_Attrs_Delete,
		Key:     it.Key,
		Created: timems(),
	}
	return StdProto.Encode(logMeta)
}

func (it *DeleteProposalRequest) LogEncode(id, ver uint64) ([]byte, error) {
	logMeta := &LogMeta{
		Id:      id,
		Version: ver,
		Attrs:   it.Attrs | Write_Attrs_Delete,
		Key:     it.Key,
		Created: timems(),
	}
	return StdProto.Encode(logMeta)
}

func NewBatchRequest() *BatchRequest {
	return &BatchRequest{}
}

func (it *BatchRequest) Delete(key []byte) {
	it.Items = append(it.Items, &RequestUnion{
		Value: &RequestUnion_Delete{
			Delete: &DeleteRequest{
				Key: key,
			},
		},
	})
}

func (it *BatchRequest) Valid() error {

	for _, req := range it.Items {

		if req.Value == nil {
			return errors.New("request empty")
		}

		reqDatabase := ""

		switch req.Value.(type) {
		case *RequestUnion_Write:
			reqDatabase = req.Value.(*RequestUnion_Write).Write.Database

		case *RequestUnion_Delete:
			reqDatabase = req.Value.(*RequestUnion_Delete).Delete.Database

		case *RequestUnion_Read:
			reqDatabase = req.Value.(*RequestUnion_Read).Read.Database

		case *RequestUnion_Range:
			reqDatabase = req.Value.(*RequestUnion_Range).Range.Database

		default:
			return errors.New("invalid request type")
		}

		if it.Database == "" && reqDatabase != "" {
			it.Database = reqDatabase
		} else if reqDatabase != "" && it.Database != reqDatabase {
			return errors.New("all subrequests can only operate on the same database")
		}
	}

	return nil
}
