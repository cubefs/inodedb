package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/server/store"
)

type Space struct {
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

func (s *Space) AddShard(ctx context.Context, shardId uint32, routeVersion uint64, inoLimit uint64, replicates map[uint32]string) {
	shard := newShard(&shardConfig{
		routeVersion: routeVersion,
		spaceId:      s.sid,
		shardId:      shardId,
		inoLimit:     inoLimit,
		replicates:   replicates,
		store:        s.store,
	})
	_, loaded := s.shards.LoadOrStore(shardId, shard)
	if loaded {
		return
	}
	s.lock.Lock()
	s.shardsTree.ReplaceOrInsert(shard.shardMeta)
	s.lock.Unlock()
	shard.Start()
}

func (s *Space) InsertItem(ctx context.Context, shardId uint32, item *proto.Item) (uint64, error) {
	if !s.validateFields(item.Fields) {
		return 0, errors.ErrUnknownField
	}
	shard, err := s.GetShard(ctx, shardId)
	if err != nil {
		return 0, err
	}

	ino, err := shard.InsertItem(ctx, item)
	if err != nil {
		return 0, err
	}
	return ino, nil
}

func (s *Space) UpdateItem(ctx context.Context, item *proto.Item) error {
	if !s.validateFields(item.Fields) {
		return errors.ErrUnknownField
	}
	shard := s.locateShard(ctx, item.Ino)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	return shard.UpdateItem(ctx, item)
}

func (s *Space) DeleteItem(ctx context.Context, ino uint64) error {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}
	return shard.DeleteItem(ctx, ino)
}

func (s *Space) GetItem(ctx context.Context, ino uint64) (*proto.Item, error) {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return nil, errors.ErrInoRangeNotFound
	}
	return shard.GetItem(ctx, ino)
}

func (s *Space) Link(ctx context.Context, link *proto.Link) error {
	shard := s.locateShard(ctx, link.Parent)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}
	return shard.Link(ctx, link)
}

func (s *Space) Unlink(ctx context.Context, unlink *proto.Unlink) error {
	shard := s.locateShard(ctx, unlink.Parent)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}
	return shard.Unlink(ctx, unlink.Parent, unlink.Name)
}

func (s *Space) List(ctx context.Context, req *proto.ListRequest) ([]*proto.Link, error) {
	shard := s.locateShard(ctx, req.Ino)
	if shard == nil {
		return nil, errors.ErrInoRangeNotFound
	}
	return shard.List(ctx, req.Ino, req.Start, req.Num)
}

func (s *Space) Search(ctx context.Context) error {
	// todo
	return nil
}

func (s *Space) GetShard(ctx context.Context, shardID uint32) (*shard, error) {
	v, ok := s.shards.Load(shardID)
	if !ok {
		return nil, errors.ErrShardDoesNotExist
	}
	return v.(*shard), nil
}

func (s *Space) locateShard(ctx context.Context, ino uint64) *shard {
	found := s.shardsTree.Get(&shardMeta{
		startIno: ino,
	})
	if found == nil {
		return nil
	}
	v, _ := s.shards.Load(found.(*shardMeta).shardId)
	return v.(*shard)
}

func (s *Space) validateFields(fields []*proto.Field) bool {
	for _, field := range fields {
		if _, ok := s.fixedFields[field.Name]; !ok {
			return false
		}
	}
	return true
}
