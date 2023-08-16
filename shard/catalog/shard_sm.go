package catalog

import (
	"context"
	"fmt"

	"github.com/cubefs/inodedb/common/kvstore"
)

func (s *shard) Apply(ctx context.Context, op RaftOp, data []byte, index uint64) (result interface{}, err error) {
	switch op {
	case RaftOpInsertItem:
		err := s.applyInsertItem(ctx, data)
		return nil, err
	default:
		panic(fmt.Sprintf("unsupported operation type: %d", op))
	}
	return nil, nil
}

func (s *shard) ApplyMemberChange(cc RaftConfChange, index uint64) error {
	return nil
}

func (s *shard) Snapshot() (RaftSnapshot, error) {
	return nil, nil
}

func (s *shard) ApplySnapshot(st RaftSnapshot) error {
	return nil
}

func (s *shard) LeaderChange(leader uint64, addr string) error {
	return nil
}

func (s *shard) applyInsertItem(ctx context.Context, data []byte) error {
	kvStore := s.store.KVStore()

	item := &item{}
	if err := item.Unmarshal(data); err != nil {
		return err
	}

	key := s.shardKeys.encodeInoKey(item.ino)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil && err != kvstore.ErrNotFound {
		return err
	}
	// already insert, just return
	if err == nil {
		vg.Close()
		return nil
	}

	return kvStore.SetRaw(ctx, dataCF, key, data, nil)
}
