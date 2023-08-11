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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenTmpPath(t *testing.T) {
	path, err := GenTmpPath()
	require.NoError(t, err)
	require.NotEqual(t, "", path)
}

func TestStringsToBytes(t *testing.T) {
	str := "test"
	b := StringsToBytes(str)
	require.Equal(t, str, string(b))
}

func TestBytesToString(t *testing.T) {
	b := []byte("test")
	str := BytesToString(b)
	require.Equal(t, str, string(b))
}

func TestGetLocalIp(t *testing.T) {
	ip, err := GetLocalIp()
	require.NoError(t, err)
	t.Log(ip)
}

func TestBufferReader(t *testing.T) {
	br := GetBufferWriter(1 << 10)
	require.Equal(t, 0, len(br.Bytes()))
	require.Equal(t, 1<<10, cap(br.Bytes()))

	PutBufferWriter(br)

	br = GetBufferWriter(1 << 10)
	require.Equal(t, 0, len(br.Bytes()))
	require.Equal(t, 1<<10, cap(br.Bytes()))
}

func TestBuffer(t *testing.T) {
	b := GetBuffer(1 << 10)
	require.Equal(t, 1<<10, len(b))

	PutBuffer(b)

	b = GetBuffer(1 << 10)
	require.Equal(t, 1<<10, len(b))
}
