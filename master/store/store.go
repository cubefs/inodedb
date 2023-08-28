package store

import (
	"C"
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
)

type DBConfig struct {
	Path          string         `json:"path"`
	RocksDbOption *RocksdbConfig `json:"rocksdb_option"`
}

type RocksdbConfig struct {
	Sync                             bool         `json:"sync"`
	ColumnFamily                     []kvstore.CF `json:"column_family"`
	CreateIfMissing                  bool         `json:"create_if_missing"`
	BlockSize                        int          `json:"block_size"`
	BlockCache                       uint64       `json:"block_cache"`
	EnablePipelinedWrite             bool         `json:"enable_pipelined_write"`
	MaxBackgroundJobs                int          `json:"max_background_jobs"`
	MaxBackgroundCompactions         int          `json:"max_background_compactions"`
	MaxBackgroundFlushes             int          `json:"max_background_flushes"`
	MaxSubCompactions                int          `json:"max_sub_compactions"`
	LevelCompactionDynamicLevelBytes bool         `json:"level_compaction_dynamic_level_bytes"`
	MaxOpenFiles                     int          `json:"max_open_files"`
	WriteConcurrency                 int          `json:"write_concurrency"`
	MinWriteBufferNumberToMerge      int          `json:"min_write_buffer_number_to_merge"`
	MaxWriteBufferNumber             int          `json:"max_write_buffer_number"`
	WriteBufferSize                  int          `json:"write_buffer_size"`
	ArenaBlockSize                   int          `json:"arena_block_size"`
	TargetFileSizeBase               uint64       `json:"target_file_size_base"`
	MaxBytesForLevelBase             uint64       `json:"max_bytes_for_level_base"`
	KeepLogFileNum                   int          `json:"keep_log_file_num"`
	MaxLogFileSize                   int          `json:"max_log_file_size"`
	Level0SlowdownWritesTrigger      int          `json:"level_0_slowdown_writes_trigger"`
	Level0StopWritesTrigger          int          `json:"level_0_stop_writes_trigger"`
	SoftPendingCompactionBytesLimit  uint64       `json:"soft_pending_compaction_bytes_limit"`
	HardPendingCompactionBytesLimit  uint64       `json:"hard_pending_compaction_bytes_limit"`
	MaxWalLogSize                    uint64       `json:"max_wal_log_size"`
}

type Store struct {
	kvStore kvstore.Store
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}

func NewStore(ctx context.Context, cfg *DBConfig) (*Store, error) {
	option := &kvstore.Option{
		Sync:                             cfg.RocksDbOption.Sync,
		ColumnFamily:                     cfg.RocksDbOption.ColumnFamily,
		CreateIfMissing:                  cfg.RocksDbOption.CreateIfMissing,
		BlockSize:                        cfg.RocksDbOption.BlockSize,
		BlockCache:                       cfg.RocksDbOption.BlockCache,
		EnablePipelinedWrite:             cfg.RocksDbOption.EnablePipelinedWrite,
		MaxBackgroundJobs:                cfg.RocksDbOption.MaxBackgroundJobs,
		MaxBackgroundCompactions:         cfg.RocksDbOption.MaxBackgroundCompactions,
		MaxBackgroundFlushes:             cfg.RocksDbOption.MaxBackgroundFlushes,
		MaxSubCompactions:                cfg.RocksDbOption.MaxSubCompactions,
		LevelCompactionDynamicLevelBytes: cfg.RocksDbOption.LevelCompactionDynamicLevelBytes,
		MaxOpenFiles:                     cfg.RocksDbOption.MaxOpenFiles,
		WriteConcurrency:                 cfg.RocksDbOption.WriteConcurrency,
		MinWriteBufferNumberToMerge:      cfg.RocksDbOption.MinWriteBufferNumberToMerge,
		MaxWriteBufferNumber:             cfg.RocksDbOption.MaxWriteBufferNumber,
		WriteBufferSize:                  cfg.RocksDbOption.WriteBufferSize,
		ArenaBlockSize:                   cfg.RocksDbOption.ArenaBlockSize,
		TargetFileSizeBase:               cfg.RocksDbOption.TargetFileSizeBase,
		MaxBytesForLevelBase:             cfg.RocksDbOption.MaxBytesForLevelBase,
		KeepLogFileNum:                   cfg.RocksDbOption.KeepLogFileNum,
		MaxLogFileSize:                   cfg.RocksDbOption.MaxLogFileSize,
		Level0SlowdownWritesTrigger:      cfg.RocksDbOption.Level0SlowdownWritesTrigger,
		Level0StopWritesTrigger:          cfg.RocksDbOption.Level0StopWritesTrigger,
		SoftPendingCompactionBytesLimit:  cfg.RocksDbOption.SoftPendingCompactionBytesLimit,
		HardPendingCompactionBytesLimit:  cfg.RocksDbOption.HardPendingCompactionBytesLimit,
		MaxWalLogSize:                    cfg.RocksDbOption.MaxWalLogSize,
	}
	kv, err := kvstore.NewKVStore(ctx, cfg.Path, kvstore.RocksdbLsmKVType, option)
	if err != nil {
		return nil, err
	}
	return &Store{kvStore: kv}, nil
}
