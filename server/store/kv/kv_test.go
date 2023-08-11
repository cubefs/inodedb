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

package kv

import (
	"context"
	"os"
	"testing"

	"github.com/cubefs/inodedb/util"
	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	ctx := context.TODO()
	path, err := util.GenTmpPath()
	require.NoError(t, err)
	defer os.RemoveAll(path)
	opt := new(Option)
	opt.CreateIfMissing = true
	eg, err := NewKV(ctx, path, RocksdbEngineType, opt)
	require.NoError(t, err)
	eg.Close()
}

func TestNewWriteBufferManager(t *testing.T) {
	mgr1 := NewWriteBufferManager(context.TODO(), RocksdbEngineType, 1<<10)
	mgr1.Close()

	mgr2 := NewWriteBufferManager(context.TODO(), "", 1<<10)
	require.Equal(t, nil, mgr2)
}

func TestNewCache(t *testing.T) {
	cache1 := NewCache(context.TODO(), RocksdbEngineType, 1<<10)
	cache1.Close()

	cache2 := NewCache(context.TODO(), "", 1<<10)
	require.Equal(t, nil, cache2)
}

func TestNewRateLimiter(t *testing.T) {
	lmt1 := NewRateLimiter(context.TODO(), RocksdbEngineType, 1<<10)
	lmt1.Close()

	lmt2 := NewRateLimiter(context.TODO(), "", 1<<10)
	require.Equal(t, nil, lmt2)
}

func TestNewEnv(t *testing.T) {
	ctx := context.TODO()
	NewEnv(ctx, RocksdbEngineType)
}

func TestNewSstFileManager(t *testing.T) {
	ctx := context.TODO()
	env := NewEnv(ctx, RocksdbEngineType)
	NewSstFileManager(ctx, RocksdbEngineType, env)
}
