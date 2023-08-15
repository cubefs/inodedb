package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/server/store"
)

type space struct {
	// read only
	sid         uint64
	name        string
	spaceType   proto.SpaceType
	fixedFields map[string]proto.FieldMeta

	shards     sync.Map
	shardsTree btree.BTree
	store      *store.Store
	lock       sync.RWMutex
}

func (s *space) AddShard(ctx context.Context, shardId uint32, inoLimit uint64, replicates map[uint32]string) {
	shard := newShard(s.sid, shardId, inoLimit, replicates, s.store)
	_, loaded := s.shards.LoadOrStore(shardId, shard)
	if loaded {
		return
	}
	s.lock.Lock()
	s.shardsTree.ReplaceOrInsert(shard.shardMeta)
	s.lock.Unlock()
	shard.Start()
}

func (s *space) GetShard(ctx context.Context, shardID uint32) (*shard, error) {
	v, ok := s.shards.Load(shardID)
	if !ok {
		return nil, errors.ErrShardDoesNotExist
	}
	return v.(*shard), nil
}

func (s *space) LocateShard(ctx context.Context, ino uint64) *shard {
	found := s.shardsTree.Get(&shardMeta{
		startIno: ino,
	})
	if found == nil {
		return nil
	}
	v, _ := s.shards.Load(found.(*shardMeta).shardId)
	return v.(*shard)
}

func (s *space) ValidateFields(fields []*proto.Field) bool {
	for _, field := range fields {
		if _, ok := s.fixedFields[field.Name]; !ok {
			return false
		}
	}
	return true
}
