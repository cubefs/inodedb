package store

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
)

type Config struct {
	Path     string         `json:"path"`
	KVOption kvstore.Option `json:"kv_option"`
}

type Store struct {
	kvStore kvstore.Store
}

func NewStore(ctx context.Context, cfg *Config) (*Store, error) {
	kvStorePath := cfg.Path + "/kv"
	kvStore, err := kvstore.NewKVStore(ctx, kvStorePath, kvstore.RocksdbLsmKVType, &cfg.KVOption)
	if err != nil {
		return nil, err
	}

	return &Store{kvStore: kvStore}, nil
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}
