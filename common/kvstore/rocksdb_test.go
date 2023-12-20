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

package kvstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/inodedb/util"
	"github.com/stretchr/testify/require"
	"github.com/tecbot/gorocksdb"
)

type testEg struct {
	engine Store
	path   string
	opt    *Option
}

func newEngine(ctx context.Context, opt *Option) (*testEg, error) {
	path, err := util.GenTmpPath()
	if err != nil {
		return nil, err
	}
	var _opt *Option
	if opt != nil {
		_opt = opt
	} else {
		_opt = new(Option)
	}
	_opt.CreateIfMissing = true
	_opt.Sync = true
	engine, err := newRocksdb(ctx, path, _opt)
	if err != nil {
		return nil, err
	}
	return &testEg{
		engine: engine,
		path:   path,
		opt:    _opt,
	}, nil
}

func (eg *testEg) close() {
	eg.engine.Close()
	os.RemoveAll(eg.path)
}

func Test_openRocksdb(t *testing.T) {
	ctx := context.TODO()
	path, err := util.GenTmpPath()
	require.NoError(t, err)
	defer os.RemoveAll(path)
	opt := new(Option)
	opt.CreateIfMissing = true
	opt.CompactionOptionFIFO = CompactionOptionFIFO{
		MaxTableFileSize: 1 << 10,
		AllowCompaction:  false,
	}
	opt.BlockSize = 1 << 20
	opt.BlockCache = 1 << 20
	opt.MaxSubCompactions = 8
	opt.MaxBackgroundJobs = 8
	opt.MaxBackgroundCompactions = 8
	opt.KeepLogFileNum = 10000
	opt.MaxLogFileSize = 1 << 30
	opt.ColumnFamily = []CF{"a", "b", "c"}
	opt.CompactionStyle = FIFOStyle
	eg, err := newRocksdb(ctx, path, opt)
	require.NoError(t, err)
	eg.Close()

	// open with empty path
	_, err = newRocksdb(ctx, "", opt)
	require.Equal(t, errors.New("path is empty"), err)
	// reopen db
	eg, err = newRocksdb(ctx, path, opt)
	require.NoError(t, err)
	eg.Close()
	// open with wrong cf
	opt.ColumnFamily = []CF{"a", "b"}
	_, err = newRocksdb(ctx, path, opt)
	require.Error(t, err)
}

func TestInstance_CreateColumn(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	err = eg.engine.CreateColumn("colA")
	require.NoError(t, err)
	cols := eg.engine.GetAllColumns()
	fmt.Println(cols)
}

func TestInstance_SetGetRaw(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	k := []byte("key1")
	v := []byte("value1")
	err = eg.engine.SetRaw(ctx, defaultCF, k, v, nil)
	require.NoError(t, err)
	v1, err := eg.engine.GetRaw(ctx, defaultCF, k, nil)
	require.NoError(t, err)
	v2, err := eg.engine.Get(ctx, defaultCF, k, nil)
	require.NoError(t, err)
	require.Equal(t, v, v1)
	require.Equal(t, v, v2.Value())
	err = eg.engine.Delete(ctx, defaultCF, k, nil)
	require.NoError(t, err)
}

func TestWrite(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	col1 := CF("c1")
	eg.engine.CreateColumn(col1)

	for i := 0; i < 5; i++ {
		keyStr := []byte(fmt.Sprintf("k%d", i))
		valStr := []byte(fmt.Sprintf("v%d", i))
		err := eg.engine.SetRaw(ctx, col1, keyStr, valStr, nil)
		require.NoError(t, err)
	}

	batch := eg.engine.NewWriteBatch()
	batch.DeleteRange(col1, []byte("k0"), []byte("k5"))
	err = eg.engine.Write(ctx, batch, nil)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		keyStr := []byte(fmt.Sprintf("k%d", i))
		_, err = eg.engine.GetRaw(ctx, col1, keyStr, nil)
		require.Equal(t, ErrNotFound, err)
	}
}

func TestRead(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	col1 := CF("c1")
	eg.engine.CreateColumn(col1)

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")

	err = eg.engine.SetRaw(ctx, col1, k1, v1, nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, "", k2, v2, nil)
	require.NoError(t, err)

	_, err = eg.engine.Read(ctx, []CF{col1, "", ""}, [][]byte{k1, k2, k3}, nil)
	require.NoError(t, err)
}

func Test_ShareCache(t *testing.T) {
	ctx := context.TODO()
	opt1 := new(Option)
	opt2 := new(Option)
	cache := NewCache(ctx, RocksdbLsmKVType, 1<<20)
	defer cache.Close()
	opt1.Cache = cache
	opt2.Cache = cache

	eg1, err := newEngine(ctx, opt1)
	require.NoError(t, err)
	eg2, err := newEngine(ctx, opt2)
	require.NoError(t, err)
	defer eg1.close()
	defer eg2.close()
}

func Test_ShareWriteBufferManager(t *testing.T) {
	ctx := context.TODO()
	opt1 := new(Option)
	opt2 := new(Option)
	manager := NewWriteBufferManager(ctx, RocksdbLsmKVType, 1<<20)
	defer manager.Close()
	opt1.WriteBufferManager = manager
	opt2.WriteBufferManager = manager

	eg1, err := newEngine(ctx, opt1)
	require.NoError(t, err)
	eg2, err := newEngine(ctx, opt2)
	require.NoError(t, err)
	defer eg1.close()
	defer eg2.close()
}

func Test_RateLimiter(t *testing.T) {
	ctx := context.TODO()
	opt := new(Option)
	rl := NewRateLimiter(ctx, RocksdbLsmKVType, 1<<20)
	defer rl.Close()
	opt.IOWriteRateLimiter = rl
	eg, err := newEngine(ctx, opt)
	require.NoError(t, err)

	oph := eg.engine.GetOptionHelper()
	err = oph.SetIOWriteRateLimiter(1 << 30)
	require.NoError(t, err)
	defer eg.close()
}

func TestOptHelper_SetGetOpts(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	oph := eg.engine.GetOptionHelper()
	require.NoError(t, oph.SetMaxBackgroundJobs(10))
	require.NoError(t, oph.SetMaxBackgroundCompactions(8))
	require.NoError(t, oph.SetMaxSubCompactions(8))
	require.NoError(t, oph.SetMaxOpenFiles(5000))
	require.NoError(t, oph.SetMaxWriteBufferNumber(36))
	require.NoError(t, oph.SetWriteBufferSize(256<<20))
	require.NoError(t, oph.SetArenaBlockSize(64<<20))
	require.NoError(t, oph.SetTargetFileSizeBase(64<<20))
	require.NoError(t, oph.SetMaxBytesForLevelBase(64<<20))
	require.NoError(t, oph.SetLevel0StopWritesTrigger(42))
	require.NoError(t, oph.SetLevel0SlowdownWritesTrigger(42))
	require.NoError(t, oph.SetSoftPendingCompactionBytesLimit(64<<20))
	require.NoError(t, oph.SetHardPendingCompactionBytesLimit(128<<20))
	require.NoError(t, oph.SetBlockSize(4096))
	require.NoError(t, oph.SetFIFOCompactionMaxTableFileSize(128<<20))
	require.NoError(t, oph.SetFIFOCompactionAllow(true))
	require.Equal(t, oph.GetOption(), *eg.opt)
}

func TestInstance_NewReadOption(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	ro := eg.engine.NewReadOption()
	k := []byte("key1")
	v := []byte("value1")
	err = eg.engine.SetRaw(ctx, defaultCF, k, v, nil)
	require.NoError(t, err)
	v1, err := eg.engine.Get(ctx, defaultCF, k, nil)
	require.NoError(t, err)
	snap := eg.engine.NewSnapshot()
	ro.SetSnapShot(snap)
	v2, err := eg.engine.Get(ctx, defaultCF, k, ro)
	require.NoError(t, err)
	require.Equal(t, v, v1.Value())
	require.Equal(t, v, v2.Value())
	ro.Close()
}

func TestValueGetter_Read(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	k := []byte("key")
	err = eg.engine.SetRaw(ctx, defaultCF, k, []byte("helloworld"), nil)
	require.NoError(t, err)
	vg, err := eg.engine.Get(ctx, defaultCF, k, nil)
	require.NoError(t, err)
	defer vg.Close()
	b := make([]byte, vg.Size()/2)
	n, err := vg.Read(b)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), b)
	require.Equal(t, vg.Size()/2, n)
	n, err = vg.Read(b)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), b)
	require.Equal(t, vg.Size()/2, n)
	n, err = vg.Read(b)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestInstance_NewWriteOption(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	wo := eg.engine.NewWriteOption()
	wo.SetSync(false)
	wo.DisableWAL(true)
	k := []byte("key1")
	v := []byte("value1")
	err = eg.engine.SetRaw(ctx, defaultCF, k, v, wo)
	require.NoError(t, err)
	v1, err := eg.engine.Get(ctx, defaultCF, k, nil)
	require.NoError(t, err)
	require.Equal(t, v, v1.Value())
	wo.Close()
}

func TestInstance_List(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	err = eg.engine.SetRaw(ctx, defaultCF, []byte("key1"), []byte("value1"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("word1"), []byte("w1"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("key2"), []byte("value2"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("check"), []byte("0"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("word2"), []byte("w2"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("key3"), []byte("value3"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("word3"), []byte("w3"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("xyz"), []byte("zyx"), nil)
	require.NoError(t, err)
	err = eg.engine.SetRaw(ctx, defaultCF, []byte("key4"), []byte("value4"), nil)
	require.NoError(t, err)

	ls := eg.engine.List(ctx, defaultCF, []byte("word"), nil, nil)
	ls.SetFilterKey([]byte("check"))
	ls.SeekTo([]byte("word2"))
	kg, vg, err := ls.ReadNext()
	require.NoError(t, err)
	require.Equal(t, []byte("word2"), kg.Key())
	require.Equal(t, []byte("w2"), vg.Value())
	kg, vg, err = ls.ReadNext()
	require.NoError(t, err)
	require.Equal(t, []byte("word3"), kg.Key())
	require.Equal(t, []byte("w3"), vg.Value())

	ls = eg.engine.List(ctx, defaultCF, []byte("key"), nil, nil)
	ls.SetFilterKey([]byte("check"))

	// prefix read
	i := 0
	for {
		i++
		kg, vg, err := ls.ReadNext()
		if kg == nil {
			i = 0
			break
		}
		require.NoError(t, err)
		require.Equal(t, []byte("key"+strconv.Itoa(i)), kg.Key())
		require.Equal(t, []byte("value"+strconv.Itoa(i)), vg.Value())
		kg.Close()
		vg.Close()
	}
	// ls.SeekToPrefix([]byte("word"))
	for {
		i++
		kg, vg, err := ls.ReadNext()
		require.NoError(t, err)
		if kg == nil {
			break
		}
		require.Equal(t, []byte("word"+strconv.Itoa(i)), kg.Key())
		require.Equal(t, []byte("w"+strconv.Itoa(i)), vg.Value())
		kg.Close()
		vg.Close()
	}
	ls.Close()
	// marker read
	ls = eg.engine.List(ctx, defaultCF, []byte("key"), []byte("key2"), nil)
	_, v, err := ls.ReadNextCopy()
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v)

	// read last
	_, vg, err = ls.ReadLast()
	require.NoError(t, err)
	require.Equal(t, []byte("value4"), vg.Value())
	require.Equal(t, 6, vg.Size())

	// nil prefix read
	ls = eg.engine.List(ctx, defaultCF, nil, nil, nil)
	// nil prefix read last
	_, vg, err = ls.ReadLast()
	require.NoError(t, err)
	require.Equal(t, []byte("zyx"), vg.Value())
	require.Equal(t, 3, vg.Size())
	vg.Close()
	ls.Close()
}

func TestInstance_Stats(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	eg.engine.FlushCF(ctx, defaultCF)
	stats, err := eg.engine.Stats(ctx)
	require.NoError(t, err)
	fmt.Println(stats.Used/(1<<10), "kb")
}

func TestEnv_SetLowPriorityBackgroundThreads(t *testing.T) {
	ctx := context.TODO()
	env := NewEnv(ctx, RocksdbLsmKVType)
	env.SetLowPriorityBackgroundThreads(1)
	env.Close()
}

func TestSstFileManager_Close(t *testing.T) {
	ctx := context.TODO()
	mgr := NewSstFileManager(ctx, RocksdbLsmKVType, NewEnv(ctx, RocksdbLsmKVType))
	mgr.Close()
}

func TestInstance_DeleteRange(t *testing.T) {
	ctx := context.TODO()
	eg, err := newEngine(ctx, nil)
	require.NoError(t, err)
	defer eg.close()

	keys := [][]byte{[]byte("/k1/a"), []byte("/k1/b"), []byte("/k1/c"), []byte("/k10"), []byte("/k1012"), []byte("/k11")}
	for _, key := range keys {
		err = eg.engine.SetRaw(ctx, defaultCF, key, []byte("1"), nil)
		require.NoError(t, err)
	}
	for _, key := range keys {
		value, err := eg.engine.Get(ctx, defaultCF, key, nil)
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value.Value())
		value.Close()
	}

	rocksdb := eg.engine.(*rocksdb)
	batch := gorocksdb.NewWriteBatch()
	start := []byte("/k1/")
	end := []byte("/k1/")
	end[len(end)-1]++
	t.Log("start: ", start, " end: ", end)
	t.Log("start: ", string(start), " end: ", string(end))
	batch.DeleteRangeCF(rocksdb.getColumnFamily(defaultCF), start, end)
	err = rocksdb.db.Write(rocksdb.writeOpt, batch)
	require.NoError(t, err)

	for _, key := range [][]byte{[]byte("/k1/a"), []byte("/k1/b"), []byte("/k1/c")} {
		_, err := eg.engine.Get(ctx, defaultCF, key, nil)
		t.Log(key, err)
		require.Equal(t, ErrNotFound, err)
	}
	for _, key := range [][]byte{[]byte("/k10"), []byte("/k1012"), []byte("/k11")} {
		value, err := eg.engine.Get(ctx, defaultCF, key, nil)
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value.Value())
		value.Close()
	}
}
