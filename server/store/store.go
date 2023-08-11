package store

import "github.com/cubefs/inodedb/common/kvstore"

type Store struct {
	kvStore kvstore.Store
}

func (s *Store) KVStore() kvstore.Store {
	return s.kvStore
}
