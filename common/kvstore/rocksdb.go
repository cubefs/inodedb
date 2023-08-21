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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	rdb "github.com/tecbot/gorocksdb"
)

type (
	rocksdb struct {
		path      string
		db        *rdb.DB
		optHelper *optHelper
		opt       *rdb.Options
		readOpt   *rdb.ReadOptions
		writeOpt  *rdb.WriteOptions
		flushOpt  *rdb.FlushOptions
		cfHandles map[CF]*rdb.ColumnFamilyHandle
		lock      sync.RWMutex
	}
	lruCache struct {
		cache *rdb.Cache
	}
	writeBufferManager struct {
		manager *rdb.WriteBufferManager
	}
	rateLimiter struct {
		limiter *rdb.RateLimiter
	}
	optHelper struct {
		db   *rdb.DB
		opt  *Option
		lock sync.RWMutex
	}
	snapshot struct {
		db   *rdb.DB
		snap *rdb.Snapshot
	}
	readOption struct {
		db   *rdb.DB
		snap *rdb.Snapshot
		opt  *rdb.ReadOptions
	}
	writeOption struct {
		opt *rdb.WriteOptions
	}
	listReader struct {
		iterator      *rdb.Iterator
		prefix        []byte
		filterKeys    [][]byte
		filterKeysTmp [][]byte
		isFirst       bool
	}
	keyGetter struct {
		key *rdb.Slice
	}
	valueGetter struct {
		index int
		value *rdb.Slice
	}
	env struct {
		*rdb.Env
	}
	sstFileManager struct {
		*rdb.SstFileManager
	}
	writeBatch struct {
		s     *rocksdb
		batch *rdb.WriteBatch
	}
)

func newRocksdb(ctx context.Context, path string, option *Option) (Store, error) {
	if path == "" {
		return nil, errors.New("path is empty")
	}
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		return nil, err
	}

	dbOpt := genRocksdbOpts(option)

	cfNum := len(option.ColumnFamily) + 1
	cols := make([]CF, 0, cfNum)
	cols = append(cols, defaultCF)
	cols = append(cols, option.ColumnFamily...)

	cfNames := make([]string, 0, cfNum)
	cfOpts := make([]*rdb.Options, 0, cfNum)
	for i := 0; i < cfNum; i++ {
		cfNames = append(cfNames, cols[i].String())
		cfOpts = append(cfOpts, dbOpt)
	}

	db, cfhs, err := rdb.OpenDbColumnFamilies(dbOpt, path, cfNames, cfOpts)
	if err != nil {
		return nil, err
	}

	cfhMap := make(map[CF]*rdb.ColumnFamilyHandle)
	for i, h := range cfhs {
		cfhMap[cols[i]] = h
	}

	wo := rdb.NewDefaultWriteOptions()
	if option.Sync {
		wo.SetSync(option.Sync)
	}
	ro := rdb.NewDefaultReadOptions()

	ins := &rocksdb{
		db:        db,
		path:      path,
		optHelper: &optHelper{db: db, opt: option},
		opt:       dbOpt,
		readOpt:   ro,
		writeOpt:  wo,
		flushOpt:  rdb.NewDefaultFlushOptions(),
		cfHandles: cfhMap,
	}
	return ins, nil
}

func newRocksdbLruCache(ctx context.Context, size uint64) LruCache {
	return &lruCache{
		cache: rdb.NewLRUCache(size),
	}
}

func (c *lruCache) GetUsage() uint64 {
	return c.cache.GetUsage()
}

func (c *lruCache) GetPinnedUsage() uint64 {
	return c.cache.GetPinnedUsage()
}

func (c *lruCache) Close() {
	c.cache.Destroy()
}

func newRocksdbWriteBufferManager(ctx context.Context, bufferSize uint64) WriteBufferManager {
	return &writeBufferManager{
		manager: rdb.NewWriteBufferManager(bufferSize),
	}
}

func (m *writeBufferManager) Close() {
	m.manager.Destroy()
}

func newRocksdbRateLimiter(ctx context.Context, rate_bytes_per_sec int64) RateLimiter {
	return &rateLimiter{
		limiter: rdb.NewRateLimiter(rate_bytes_per_sec, 10000, 3),
	}
}

func newRocksdbEnv(ctx context.Context) Env {
	return &env{rdb.NewDefaultEnv()}
}

func (e *env) SetLowPriorityBackgroundThreads(n int) {
	e.SetBackgroundThreads(n)
}

func (e *env) Close() {
	e.Destroy()
}

func newRocksdbSstFileManager(ctx context.Context, e Env) SstFileManager {
	return &sstFileManager{rdb.NewSstFileManager(e.(*env).Env)}
}

func (e *sstFileManager) Close() {
	e.Destroy()
}

func (l *rateLimiter) SetBytesPerSec(value int64) {
	l.limiter.SetBytesPerSecond(value)
}

func (l *rateLimiter) Close() {
	l.limiter.Destroy()
}

func (ss *snapshot) Close() {
	ss.db.ReleaseSnapshot(ss.snap)
}

func (ro *readOption) SetSnapShot(snap Snapshot) {
	ro.snap = snap.(*snapshot).snap
	ro.opt.SetSnapshot(ro.snap)
}

func (ro *readOption) Close() {
	ro.opt.Destroy()
}

func (wo *writeOption) SetSync(value bool) {
	wo.opt.SetSync(value)
}

func (wo *writeOption) DisableWAL(value bool) {
	wo.opt.DisableWAL(value)
}

func (wo *writeOption) Close() {
	wo.opt.Destroy()
}

func (kg keyGetter) Key() []byte {
	return kg.key.Data()
}

func (kg keyGetter) Close() {
	kg.key.Free()
}

func (vg *valueGetter) Value() []byte {
	return vg.value.Data()
}

func (vg *valueGetter) Read(b []byte) (n int, err error) {
	if vg.index >= len(vg.Value()) {
		return 0, io.EOF
	}
	n = copy(b, vg.Value()[vg.index:])
	vg.index += n
	return
}

func (vg *valueGetter) Size() int {
	return vg.value.Size()
}

func (vg *valueGetter) Close() error {
	vg.value.Free()
	return nil
}

func (lr *listReader) ReadNext() (key KeyGetter, val ValueGetter, err error) {
	if lr.isFirst {
		if err = lr.iterator.Err(); err != nil {
			return nil, nil, err
		}
		if !lr.iterator.Valid() {
			return nil, nil, nil
		}
		if lr.prefix == nil || lr.iterator.ValidForPrefix(lr.prefix) {
			kg := keyGetter{key: lr.iterator.Key()}
			vg := &valueGetter{value: lr.iterator.Value()}
			lr.isFirst = false
			if lr.filterKey(kg) {
				lr.removeFilterKey(kg)
				return lr.ReadNext()
			}
			return kg, vg, nil
		}
		return nil, nil, nil
	}
	// move into next kv
	lr.iterator.Next()
	if err = lr.iterator.Err(); err != nil {
		return nil, nil, err
	}
	if !lr.iterator.Valid() {
		return nil, nil, nil
	}
	if lr.prefix == nil || lr.iterator.ValidForPrefix(lr.prefix) {
		kg := keyGetter{key: lr.iterator.Key()}
		vg := &valueGetter{value: lr.iterator.Value()}
		if lr.filterKey(kg) {
			lr.removeFilterKey(kg)
			return lr.ReadNext()
		}
		return kg, vg, nil
	}
	return nil, nil, nil
}

func (lr *listReader) ReadNextCopy() (key []byte, value []byte, err error) {
	kg, vg, err := lr.ReadNext()
	if err != nil {
		return nil, nil, err
	}
	if kg != nil && vg != nil {
		key = make([]byte, len(kg.Key()))
		value = make([]byte, vg.Size())
		copy(key, kg.Key())
		copy(value, vg.Value())
		kg.Close()
		vg.Close()
		return
	}
	return
}

func (lr *listReader) ReadLast() (key KeyGetter, val ValueGetter, err error) {
	if lr.prefix == nil {
		lr.iterator.SeekToLast()
		if err = lr.iterator.Err(); err != nil {
			return
		}
		if !lr.iterator.Valid() {
			return
		}
		key = keyGetter{key: lr.iterator.Key()}
		val = &valueGetter{value: lr.iterator.Value()}
		return
	}
	for {
		if err = lr.iterator.Err(); err != nil {
			return
		}
		if !lr.iterator.Valid() {
			return
		}
		if !lr.iterator.ValidForPrefix(lr.prefix) {
			lr.iterator.Prev()
			break
		}
		lr.iterator.Next()
	}
	key = keyGetter{key: lr.iterator.Key()}
	val = &valueGetter{value: lr.iterator.Value()}
	return
}

func (lr *listReader) SeekTo(key []byte) {
	lr.isFirst = true
	lr.prefix = nil
	lr.iterator.Seek(key)
	for i := range lr.filterKeysTmp {
		lr.SetFilterKey(lr.filterKeysTmp[i])
	}
}

func (lr *listReader) SeekToPrefix(prefix []byte) {
	lr.isFirst = true
	lr.prefix = prefix
	lr.iterator.Seek(prefix)
	for i := range lr.filterKeysTmp {
		lr.SetFilterKey(lr.filterKeysTmp[i])
	}
}

func (lr *listReader) SetFilterKey(key []byte) {
	lr.filterKeys = append(lr.filterKeys, key)
}

func (lr *listReader) Close() {
	lr.iterator.Close()
}

func (lr *listReader) filterKey(kg keyGetter) bool {
	if lr.filterKeys != nil {
		for i := range lr.filterKeys {
			// nolint
			return bytes.Compare(lr.filterKeys[i], kg.Key()) == 0
		}
	}
	return false
}

func (lr *listReader) removeFilterKey(kg keyGetter) {
	if lr.filterKeys != nil {
		for i := range lr.filterKeys {
			if bytes.Equal(lr.filterKeys[i], kg.Key()) {
				lr.filterKeysTmp = append(lr.filterKeysTmp, lr.filterKeys[i])
				lr.filterKeys = append(lr.filterKeys[:i], lr.filterKeys[i+1:]...)
			}
		}
	}
}

func (w *writeBatch) Put(col CF, key, value []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.PutCF(cf, key, value)
}

func (w *writeBatch) Delete(col CF, key []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.DeleteCF(cf, key)
}

func (w *writeBatch) DeleteRange(col CF, startKey, endKey []byte) {
	cf := w.s.getColumnFamily(col)
	w.batch.DeleteRangeCF(cf, startKey, endKey)
}

func (w *writeBatch) Data() []byte {
	return w.batch.Data()
}

func (w *writeBatch) From(data []byte) {
	w.batch = rdb.WriteBatchFrom(data)
}

func (w *writeBatch) Close() {
	w.batch.Destroy()
}

func (s *rocksdb) NewSnapshot() Snapshot {
	return &snapshot{db: s.db, snap: s.db.NewSnapshot()}
}

func (s *rocksdb) NewReadOption() ReadOption {
	opt := rdb.NewDefaultReadOptions()
	return &readOption{
		db:  s.db,
		opt: opt,
	}
}

func (s *rocksdb) NewWriteOption() WriteOption {
	return &writeOption{
		opt: rdb.NewDefaultWriteOptions(),
	}
}

func (s *rocksdb) NewWriteBatch() WriteBatch {
	return &writeBatch{
		s:     s,
		batch: rdb.NewWriteBatch(),
	}
}

func (s *rocksdb) CreateColumn(col CF) error {
	s.lock.Lock()
	if s.cfHandles[col] != nil {
		s.lock.Unlock()
		return nil
	}
	h, err := s.db.CreateColumnFamily(s.opt, col.String())
	if err != nil {
		s.lock.Unlock()
		return err
	}
	s.cfHandles[col] = h
	s.lock.Unlock()
	return nil
}

func (s *rocksdb) GetAllColumns() (ret []CF) {
	s.lock.RLock()
	for col := range s.cfHandles {
		ret = append(ret, col)
	}
	s.lock.RUnlock()
	return
}

func (s *rocksdb) Get(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value ValueGetter, err error) {
	var v *rdb.Slice
	cf := s.getColumnFamily(col)
	ro := s.readOpt
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	if v, err = s.db.GetCF(ro, cf, key); err != nil {
		return nil, err
	}
	if !v.Exists() {
		return nil, ErrNotFound
	}
	value = &valueGetter{value: v}
	return value, err
}

func (s *rocksdb) GetRaw(ctx context.Context, col CF, key []byte, readOpt ReadOption) (value []byte, err error) {
	var v *rdb.Slice
	cf := s.getColumnFamily(col)
	ro := s.readOpt
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	if v, err = s.db.GetCF(ro, cf, key); err != nil {
		return nil, err
	}
	if !v.Exists() {
		return nil, ErrNotFound
	}
	value = make([]byte, v.Size())
	copy(value, v.Data())
	v.Free()
	return value, nil
}

func (s *rocksdb) SetRaw(ctx context.Context, col CF, key []byte, value []byte, writeOpt WriteOption) error {
	wo := s.writeOpt
	cf := s.getColumnFamily(col)
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	if err := s.db.PutCF(wo, cf, key, value); err != nil {
		return err
	}
	return nil
}

func (s *rocksdb) Delete(ctx context.Context, col CF, key []byte, writeOpt WriteOption) error {
	wo := s.writeOpt
	cf := s.getColumnFamily(col)
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	if err := s.db.DeleteCF(wo, cf, key); err != nil {
		return err
	}
	return nil
}

func (s *rocksdb) List(ctx context.Context, col CF, prefix []byte, marker []byte, readOpt ReadOption) ListReader {
	cf := s.getColumnFamily(col)

	ro := s.readOpt
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	t := s.db.NewIteratorCF(ro, cf)
	if len(marker) > 0 {
		t.Seek(marker)
	} else {
		if prefix != nil {
			t.Seek(prefix)
		} else {
			t.SeekToFirst()
		}
	}

	lr := &listReader{
		iterator: t,
		prefix:   prefix,
		isFirst:  true,
	}
	return lr
}

func (s *rocksdb) Write(ctx context.Context, batch WriteBatch, writeOpt WriteOption) error {
	wo := s.writeOpt
	if writeOpt != nil {
		wo = writeOpt.(*writeOption).opt
	}
	_batch := batch.(*writeBatch)
	return s.db.Write(wo, _batch.batch)
}

func (s *rocksdb) Read(ctx context.Context, cols []CF, keys [][]byte, readOpt ReadOption) (values []ValueGetter, err error) {
	ro := s.readOpt
	if readOpt != nil {
		ro = readOpt.(*readOption).opt
	}
	cfhs := make([]*rdb.ColumnFamilyHandle, len(cols))
	for i, col := range cols {
		cfhs[i] = s.getColumnFamily(col)
	}
	_values, err := s.db.MultiGetCFMultiCF(ro, cfhs, keys)
	if err != nil {
		return nil, err
	}
	values = make([]ValueGetter, len(_values))
	for i := range _values {
		if _values[i].Data() == nil {
			values[i] = nil
			continue
		}
		values[i] = &valueGetter{value: _values[i]}
	}
	return
}

func (s *rocksdb) FlushCF(ctx context.Context, col CF) error {
	cf := s.getColumnFamily(col)
	return s.db.FlushCF(s.flushOpt, cf)
}

func (s *rocksdb) Stats(ctx context.Context) (stats Stats, err error) {
	var (
		size                     int64
		totalIndexAndFilterUsage uint64
		totalMemtableUsage       uint64
	)
	files := s.db.GetLiveFilesMetaData()
	for i := range files {
		size += files[i].Size
	}

	for _, cf := range s.cfHandles {
		indexAndFilterUsage, _ := strconv.ParseUint(s.db.GetPropertyCF("rocksdb.estimate-table-readers-mem", cf), 10, 64)
		memtableUsage, _ := strconv.ParseUint(s.db.GetPropertyCF("rocksdb.cur-size-all-mem-tables", cf), 10, 64)
		totalIndexAndFilterUsage += indexAndFilterUsage
		totalMemtableUsage += memtableUsage
	}
	blockCacheUsage, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.block-cache-usage"), 10, 64)
	blockPinnedUsage, _ := strconv.ParseUint(s.db.GetProperty("rocksdb.block-cache-pinned-usage"), 10, 64)
	stats = Stats{
		Used: uint64(size),
		MemoryUsage: MemoryUsage{
			BlockCacheUsage:     blockCacheUsage,
			IndexAndFilterUsage: totalIndexAndFilterUsage,
			MemtableUsage:       totalMemtableUsage,
			BlockPinnedUsage:    blockPinnedUsage,
			Total:               blockCacheUsage + totalIndexAndFilterUsage + totalMemtableUsage + blockPinnedUsage,
		},
	}
	return
}

func (s *rocksdb) Close() {
	s.writeOpt.Destroy()
	s.readOpt.Destroy()
	s.opt.Destroy()
	s.flushOpt.Destroy()
	for i := range s.cfHandles {
		s.cfHandles[i].Destroy()
	}
	s.db.Close()
}

func (s *rocksdb) GetOptionHelper() OptionHelper {
	return s.optHelper
}

func (oph *optHelper) GetOption() Option {
	oph.lock.RLock()
	opt := *oph.opt
	oph.lock.RUnlock()
	return opt
}

func (oph *optHelper) SetMaxBackgroundJobs(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetDBOptions([]string{"max_background_jobs"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.MaxBackgroundJobs = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxBackgroundCompactions(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetDBOptions([]string{"max_background_compactions"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.MaxBackgroundCompactions = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxSubCompactions(value int) error {
	oph.lock.Lock()
	// todo
	oph.opt.MaxSubCompactions = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxOpenFiles(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetDBOptions([]string{"max_open_files"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.MaxOpenFiles = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxWriteBufferNumber(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"max_write_buffer_number"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.MaxWriteBufferNumber = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetWriteBufferSize(size int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"write_buffer_size"}, []string{strconv.Itoa(size)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.WriteBufferSize = size
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetArenaBlockSize(size int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"arena_block_size"}, []string{strconv.Itoa(size)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.ArenaBlockSize = size
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetTargetFileSizeBase(value uint64) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"target_file_size_base"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.TargetFileSizeBase = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetMaxBytesForLevelBase(value uint64) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"max_bytes_for_level_base"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.MaxBytesForLevelBase = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetLevel0SlowdownWritesTrigger(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"level0_slowdown_writes_trigger"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.Level0SlowdownWritesTrigger = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetLevel0StopWritesTrigger(value int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"level0_stop_writes_trigger"}, []string{strconv.Itoa(value)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.Level0StopWritesTrigger = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetSoftPendingCompactionBytesLimit(value uint64) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"soft_pending_compaction_bytes_limit"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.SoftPendingCompactionBytesLimit = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetHardPendingCompactionBytesLimit(value uint64) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions([]string{"hard_pending_compaction_bytes_limit"}, []string{strconv.FormatUint(value, 10)}); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.HardPendingCompactionBytesLimit = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetBlockSize(size int) error {
	oph.lock.Lock()
	// todo
	oph.opt.BlockSize = size
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetFIFOCompactionMaxTableFileSize(size int) error {
	oph.lock.Lock()
	if err := oph.db.SetOptions(formatFIFOCompactionOption("max_table_files_size", strconv.Itoa(size))); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.CompactionOptionFIFO.MaxTableFileSize = size
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetFIFOCompactionAllow(value bool) error {
	oph.lock.Lock()
	v := "false"
	if value {
		v = "true"
	}
	if err := oph.db.SetOptions(formatFIFOCompactionOption("allow_compaction", v)); err != nil {
		oph.lock.Unlock()
		return err
	}
	oph.opt.CompactionOptionFIFO.AllowCompaction = value
	oph.lock.Unlock()
	return nil
}

func (oph *optHelper) SetIOWriteRateLimiter(value int64) error {
	oph.lock.Lock()
	if oph.opt.IOWriteRateLimiter == nil {
		oph.opt.IOWriteRateLimiter = &rateLimiter{limiter: rdb.NewRateLimiter(value, 10000, 3)}
		oph.lock.Unlock()
		return nil
	}
	oph.opt.IOWriteRateLimiter.SetBytesPerSec(value)
	oph.lock.Unlock()
	return nil
}

func (s *rocksdb) getColumnFamily(col CF) *rdb.ColumnFamilyHandle {
	if col == "" {
		col = defaultCF
	}
	s.lock.RLock()
	cf, ok := s.cfHandles[col]
	if !ok {
		s.lock.RUnlock()
		panic(fmt.Sprintf("col:%s not exist", col.String()))
	}
	s.lock.RUnlock()
	return cf
}

func (s *rocksdb) CheckColumns(col CF) bool {
	if col == "" {
		return true
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.cfHandles[col]
	return ok
}

func genRocksdbOpts(opt *Option) (opts *rdb.Options) {
	opts = rdb.NewDefaultOptions()
	blockBaseOpt := rdb.NewDefaultBlockBasedTableOptions()
	fifoCompactionOpt := rdb.NewDefaultFIFOCompactionOptions()
	opts.SetCreateIfMissing(opt.CreateIfMissing)
	if opt.BlockSize > 0 {
		blockBaseOpt.SetBlockSize(opt.BlockSize)
	}
	if opt.Cache != nil {
		blockBaseOpt.SetBlockCache(opt.Cache.(*lruCache).cache)
		// blockBaseOpt.SetCacheIndexAndFilterBlocks(true)
	} else {
		if opt.BlockCache > 0 {
			blockBaseOpt.SetBlockCache(rdb.NewLRUCache(opt.BlockCache))
		}
	}
	opts.SetEnablePipelinedWrite(opt.EnablePipelinedWrite)
	if opt.MaxBackgroundCompactions > 0 {
		opts.SetMaxBackgroundCompactions(opt.MaxBackgroundCompactions)
	}
	if opt.MaxBackgroundFlushes > 0 {
		opts.SetMaxBackgroundFlushes(opt.MaxBackgroundFlushes)
	}
	if opt.MaxSubCompactions > 0 {
		opts.SetMaxSubCompactions(opt.MaxSubCompactions)
	}

	opts.SetLevelCompactionDynamicLevelBytes(opt.LevelCompactionDynamicLevelBytes)
	if opt.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(opt.MaxOpenFiles)
	}
	if opt.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(opt.MinWriteBufferNumberToMerge)
	}
	if opt.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(opt.MaxWriteBufferNumber)
	}
	if opt.WriteBufferSize > 0 {
		opts.SetWriteBufferSize(opt.WriteBufferSize)
	}
	if opt.ArenaBlockSize > 0 {
		opts.SetArenaBlockSize(opt.ArenaBlockSize)
	}
	if opt.TargetFileSizeBase > 0 {
		opts.SetTargetFileSizeBase(opt.TargetFileSizeBase)
	}
	if opt.MaxBytesForLevelBase > 0 {
		opts.SetMaxBytesForLevelBase(opt.MaxBytesForLevelBase)
	}
	if opt.KeepLogFileNum > 0 {
		opts.SetKeepLogFileNum(opt.KeepLogFileNum)
	}
	if opt.MaxLogFileSize > 0 {
		opts.SetMaxLogFileSize(opt.MaxLogFileSize)
	}
	if opt.Level0SlowdownWritesTrigger > 0 {
		opts.SetLevel0SlowdownWritesTrigger(opt.Level0SlowdownWritesTrigger)
	}
	if opt.Level0StopWritesTrigger > 0 {
		opts.SetLevel0StopWritesTrigger(opt.Level0StopWritesTrigger)
	}
	if opt.SoftPendingCompactionBytesLimit > 0 {
		opts.SetSoftPendingCompactionBytesLimit(opt.SoftPendingCompactionBytesLimit)
	}
	if opt.HardPendingCompactionBytesLimit > 0 {
		opts.SetHardPendingCompactionBytesLimit(opt.HardPendingCompactionBytesLimit)
	}
	if len(opt.CompactionStyle) > 0 {
		switch opt.CompactionStyle {
		case FIFOStyle:
			opts.SetCompactionStyle(rdb.FIFOCompactionStyle)
		case LevelStyle:
			opts.SetCompactionStyle(rdb.LevelCompactionStyle)
		case UniversalStyle:
			opts.SetCompactionStyle(rdb.UniversalCompactionStyle)
		default:
		}
	}
	if opt.CompactionOptionFIFO.MaxTableFileSize > 0 {
		fifoCompactionOpt.SetMaxTableFilesSize(uint64(opt.CompactionOptionFIFO.MaxTableFileSize))
	}
	if opt.IOWriteRateLimiter != nil {
		opts.SetRateLimiter(opt.IOWriteRateLimiter.(*rateLimiter).limiter)
	}
	if opt.WriteBufferManager != nil {
		opts.SetWriteBufferManager(opt.WriteBufferManager.(*writeBufferManager).manager)
	}
	if opt.MaxWalLogSize > 0 {
		opts.SetMaxTotalWalSize(opt.MaxWalLogSize)
	}
	if opt.Env != nil {
		opts.SetEnv(opt.Env.(*env).Env)
	} else {
		opts.SetEnv(rdb.NewDefaultEnv())
	}
	if opt.SstFileManager != nil {
		opts.SetSstFileManager(opt.SstFileManager.(*sstFileManager).SstFileManager)
	}

	opts.SetStatsDumpPeriodSec(0)
	opts.SetStatsPersistPeriodSec(0)
	opts.SetBlockBasedTableFactory(blockBaseOpt)
	opts.SetFIFOCompactionOptions(fifoCompactionOpt)
	opts.SetCreateIfMissingColumnFamilies(true)

	return
}

func formatFIFOCompactionOption(key, value string) ([]string, []string) {
	s := fmt.Sprintf("%s=%s;", key, value)
	return []string{"compaction_options_fifo"}, []string{s}
}
