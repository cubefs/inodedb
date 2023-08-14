package store

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
)

type DBConfig struct {
	Path          string          `json:"path"`
	RocksDbOption *kvstore.Option `json:"rocks_db_option"`
}

type Store struct {
	kvStore kvstore.Store
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}

func NewStore(ctx context.Context, cfg *DBConfig) *Store {
	kv, err := kvstore.NewKVStore(ctx, cfg.Path, kvstore.RocksdbLsmKVType, cfg.RocksDbOption)
	if err != nil {
		return nil
	}
	return &Store{kvStore: kv}
}
