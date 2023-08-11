package store

import "github.com/cubefs/inodedb/server/store/kv"

type Store struct {
	kvStore kv.KVStore
}

func (s *Store) KVStore() kv.KVStore {
	return s.kvStore
}
