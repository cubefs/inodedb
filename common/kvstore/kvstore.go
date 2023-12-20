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
)

const (
	defaultCF = "default"

	RocksdbLsmKVType = LsmKVType("rocksdb")

	FIFOStyle      = CompactionStyle("fifo")
	LevelStyle     = CompactionStyle("level")
	UniversalStyle = CompactionStyle("universal")
)

var (
	ErrNotFound       = errors.New("key not found")
	ErrKVTypeNotFound = errors.New("kv type not found")
)

type (
	CF              string
	LsmKVType       string
	CompactionStyle string

	Store interface {
		NewSnapshot() Snapshot
		CreateColumn(col CF) error
		GetAllColumns() []CF
		CheckColumns(col CF) bool
		Get(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value ValueGetter, err error)
		GetRaw(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value []byte, err error)
		MultiGet(ctx context.Context, col CF, keys [][]byte, readOpt ReadOption) (values []ValueGetter, err error)
		SetRaw(ctx context.Context, col CF, key []byte, value []byte, writeOpt WriteOption) error
		Delete(ctx context.Context, col CF, key []byte, writeOpt WriteOption) error
		List(ctx context.Context, col CF, prefix []byte, marker []byte, readOpt ReadOption) ListReader
		Write(ctx context.Context, batch WriteBatch, writeOpt WriteOption) error
		Read(ctx context.Context, cols []CF, keys [][]byte, readOpt ReadOption) (values []ValueGetter, err error)
		GetOptionHelper() (helper OptionHelper)
		NewReadOption() (readOption ReadOption)
		NewWriteOption() (writeOption WriteOption)
		NewWriteBatch() (writeBatch WriteBatch)
		FlushCF(ctx context.Context, col CF) error
		Stats(ctx context.Context) (Stats, error)
		Close()
	}
	OptionHelper interface {
		GetOption() Option
		SetMaxBackgroundJobs(value int) error
		SetMaxBackgroundCompactions(value int) error
		SetMaxSubCompactions(value int) error
		SetMaxOpenFiles(value int) error
		SetMaxWriteBufferNumber(value int) error
		SetWriteBufferSize(size int) error
		SetArenaBlockSize(size int) error
		SetTargetFileSizeBase(value uint64) error
		SetMaxBytesForLevelBase(value uint64) error
		SetLevel0SlowdownWritesTrigger(value int) error
		SetLevel0StopWritesTrigger(value int) error
		SetSoftPendingCompactionBytesLimit(value uint64) error
		SetHardPendingCompactionBytesLimit(value uint64) error
		SetBlockSize(size int) error
		SetFIFOCompactionMaxTableFileSize(size int) error
		SetFIFOCompactionAllow(value bool) error
		SetIOWriteRateLimiter(value int64) error
	}
	LruCache interface {
		GetUsage() uint64
		GetPinnedUsage() uint64
		Close()
	}
	WriteBufferManager interface {
		Close()
	}
	RateLimiter interface {
		SetBytesPerSec(value int64)
		Close()
	}
	ListReader interface {
		ReadNext() (key KeyGetter, val ValueGetter, err error)
		ReadNextCopy() (key []byte, value []byte, err error)
		ReadPrev() (key KeyGetter, val ValueGetter, err error)
		ReadPrevCopy() (key []byte, value []byte, err error)
		ReadLast() (key KeyGetter, val ValueGetter, err error)
		SeekToLast()
		SeekForPrev(key []byte) (err error)
		SeekTo(key []byte)
		SetFilterKey(key []byte)
		Close()
	}
	KeyGetter interface {
		Key() []byte
		Close()
	}
	ValueGetter interface {
		Value() []byte
		Read([]byte) (n int, err error)
		Size() int
		Close()
	}
	Snapshot interface {
		Close()
	}
	ReadOption interface {
		SetSnapShot(snap Snapshot)
		Close()
	}
	WriteOption interface {
		SetSync(value bool)
		DisableWAL(value bool)
		Close()
	}
	Env interface {
		SetLowPriorityBackgroundThreads(n int)
		SetHighPriorityBackgroundThreads(n int)
		Close()
	}
	SstFileManager interface {
		Close()
	}
	WriteBatch interface {
		Put(col CF, key, value []byte)
		Delete(col CF, key []byte)
		DeleteRange(col CF, startKey, endKey []byte)
		Data() []byte
		From(data []byte)
		Close()
		// Iterator()
	}

	Stats struct {
		Used        uint64
		MemoryUsage MemoryUsage
	}
	MemoryUsage struct {
		BlockCacheUsage     uint64
		IndexAndFilterUsage uint64
		MemtableUsage       uint64
		BlockPinnedUsage    uint64
		Total               uint64
	}
	Option struct {
		Sync                             bool
		DisableWal                       bool
		ColumnFamily                     []CF `json:"column_family"`
		CreateIfMissing                  bool
		Cache                            LruCache
		BlockSize                        int
		BlockCache                       uint64
		EnablePipelinedWrite             bool
		MaxBackgroundJobs                int
		MaxBackgroundCompactions         int
		MaxBackgroundFlushes             int
		MaxSubCompactions                int
		LevelCompactionDynamicLevelBytes bool
		MaxOpenFiles                     int
		WriteConcurrency                 int
		MinWriteBufferNumberToMerge      int
		MaxWriteBufferNumber             int
		WriteBufferSize                  int
		ArenaBlockSize                   int
		TargetFileSizeBase               uint64
		MaxBytesForLevelBase             uint64
		KeepLogFileNum                   int
		MaxLogFileSize                   int
		Level0SlowdownWritesTrigger      int
		Level0StopWritesTrigger          int
		SoftPendingCompactionBytesLimit  uint64
		HardPendingCompactionBytesLimit  uint64
		MaxWalLogSize                    uint64
		CompactionStyle                  CompactionStyle
		CompactionOptionFIFO             CompactionOptionFIFO
		IOWriteRateLimiter               RateLimiter
		WriteBufferManager               WriteBufferManager
		Env                              Env
		SstFileManager                   SstFileManager
		HandleError                      HandleError
	}
	CompactionOptionFIFO struct {
		MaxTableFileSize int
		AllowCompaction  bool
	}
	HandleError func(err error)
)

func NewKVStore(ctx context.Context, path string, lsmType LsmKVType, option *Option) (Store, error) {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdb(ctx, path, option)
	default:
		return nil, ErrNotFound
	}
}

func NewCache(ctx context.Context, lsmType LsmKVType, size uint64) LruCache {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbLruCache(ctx, size)
	default:
		return nil
	}
}

func NewWriteBufferManager(ctx context.Context, lsmType LsmKVType, size uint64) WriteBufferManager {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbWriteBufferManager(ctx, size)
	default:
		return nil
	}
}

func NewRateLimiter(ctx context.Context, lsmType LsmKVType, value int64) RateLimiter {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbRateLimiter(ctx, value)
	default:
		return nil
	}
}

func NewEnv(ctx context.Context, lsmType LsmKVType) Env {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbEnv(ctx)
	default:
		return nil
	}
}

func NewSstFileManager(ctx context.Context, lsmType LsmKVType, env Env) SstFileManager {
	switch lsmType {
	case RocksdbLsmKVType:
		return newRocksdbSstFileManager(ctx, env)
	default:
		return nil
	}
}

func (cf CF) String() string {
	return string(cf)
}
