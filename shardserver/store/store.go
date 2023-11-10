package store

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
)

type Config struct {
	Path       string         `json:"path"`
	KVOption   kvstore.Option `json:"kv_option"`
	RaftOption kvstore.Option `json:"raft_option"`
}

type Store struct {
	kvStore   kvstore.Store
	raftStore kvstore.Store
	rawFS     RawFS
}

func NewStore(ctx context.Context, cfg *Config) (*Store, error) {
	kvStorePath := cfg.Path + "/kv"
	// disable kv wal to optimized latency
	cfg.KVOption.DisableWal = true
	// todo: add blobDB support for vector data
	kvStore, err := kvstore.NewKVStore(ctx, kvStorePath, kvstore.RocksdbLsmKVType, &cfg.KVOption)
	if err != nil {
		return nil, err
	}

	raftStorePath := cfg.Path + "/raft"
	raftStore, err := kvstore.NewKVStore(ctx, raftStorePath, kvstore.RocksdbLsmKVType, &cfg.RaftOption)
	if err != nil {
		return nil, err
	}

	return &Store{
		kvStore:   kvStore,
		raftStore: raftStore,
		rawFS:     &posixRawFS{path: cfg.Path + "/raw"},
	}, nil
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}

func (s *Store) RaftStore() kvstore.Store {
	return s.raftStore
}

func (s *Store) RawFS() RawFS {
	return s.rawFS
}
