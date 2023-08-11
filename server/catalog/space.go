package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/inodedb/errors"

	"github.com/cubefs/inodedb/proto"

	"github.com/cubefs/cubefs/util/btree"
)

type space struct {
	name       string
	shards     sync.Map
	shardsTree btree.BTree
}

func (s *space) AddShard(ctx context.Context, shardId uint32, inoRange *proto.InoRange, replicates map[uint32]string) {
	new := newShard(shardId, inoRange, replicates)
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
