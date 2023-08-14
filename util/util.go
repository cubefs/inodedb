// Copyright 2023 The Cuber Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"bytes"
	"errors"
	"net"
	"os"
	"reflect"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/google/uuid"
)

// GenTmpPath create a temporary path
func GenTmpPath() (string, error) {
	id := uuid.NewString()
	path := os.TempDir() + "/" + id
	if err := os.RemoveAll(path); err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

func StringsToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh)) //nolint: govet
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func GetLocalIp() (string, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addresses {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("can not find the local ip address")
}

func GetBufferWriter(size int) *bytes.Buffer {
	return bytes.NewBuffer(bytespool.Alloc(size)[:0])
}

func PutBufferWriter(br *bytes.Buffer) {
	bytespool.Free(br.Bytes())
}

func GetBuffer(size int) []byte {
	return bytespool.Alloc(size)
}

func PutBuffer(b []byte) {
	bytespool.Free(b)
}
