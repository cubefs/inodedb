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
	shards     sync.Map
	shardsTree btree.BTree
	store      *store.Store

	meta proto.SpaceMeta
}

func (s *space) AddShard(ctx context.Context, shardId uint32, inoRange *proto.InoRange, replicates map[uint32]string) {
	new := newShard(s.meta.Sid, shardId, inoRange, replicates, s.store)
	actual, loaded := s.shards.LoadOrStore(shardId, new)
	if loaded {
		return
	}
	shard := actual.(*shard)
	s.shardsTree.ReplaceOrInsert(shard.shardMeta)
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
		inoRange: &proto.InoRange{
			StartIno: ino,
		},
	})
	if found == nil {
		return nil
	}
	v, _ := s.shards.Load(found.(*shardMeta).id)
	return v.(*shard)
}
