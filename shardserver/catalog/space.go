package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/inodedb/raft"

	"github.com/cubefs/cubefs/blobstore/util/errors"

	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	"github.com/cubefs/inodedb/shardserver/store"
)

type shardHandler interface {
	GetNodeInfo() *proto.Node
	GetRaftManager() raft.Manager
	GetShardBaseConfig() *ShardBaseConfig
}

type Space struct {
	// read only
	sid         uint64
	name        string
	spaceType   proto.SpaceType
	fixedFields map[string]proto.FieldMeta

	shards sync.Map
	lock   sync.RWMutex

	store        *store.Store
	shardHandler shardHandler
}

func (s *Space) Load(ctx context.Context) error {
	listKeyPrefix := make([]byte, spacePrefixSize())
	encodeSpacePrefix(s.sid, listKeyPrefix)

	kvStore := s.store.KVStore()
	lr := kvStore.List(ctx, dataCF, listKeyPrefix, nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return errors.Info(err, "read next shard kv failed")
		}
		if kg == nil || vg == nil {
			break
		}

		shardInfo := &shardInfo{}
		if err := shardInfo.Unmarshal(vg.Value()); err != nil {
			return errors.Info(err, "unmarshal shard info failed")
		}

		shard, err := newShard(ctx, shardConfig{
			ShardBaseConfig: s.shardHandler.GetShardBaseConfig(),
			shardInfo:       *shardInfo,
			nodeInfo:        s.shardHandler.GetNodeInfo(),
			store:           s.store,
			raftManager:     s.shardHandler.GetRaftManager(),
		})
		if err != nil {
			return errors.Info(err, "new shard failed")
		}
		s.shards.Store(shardInfo.ShardId, shard)
		shard.Start()
	}

	return nil
}

func (s *Space) AddShard(ctx context.Context, shardId uint32, epoch uint64, inoLimit uint64, nodes []*proto.ShardNode) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, loaded := s.shards.Load(shardId)
	if loaded {
		return nil
	}

	shardNodes := make([]*persistent.ShardNode, len(nodes))
	for i := range nodes {
		shardNodes[i] = &persistent.ShardNode{
			Id:      nodes[i].Id,
			Learner: nodes[i].Learner,
		}
	}

	shardInfo := &shardInfo{
		ShardId:   shardId,
		Sid:       s.sid,
		InoCursor: calculateStartIno(shardId),
		InoLimit:  inoLimit,
		Epoch:     epoch,
		Nodes:     shardNodes,
	}

	/*kvStore := s.store.KVStore()
	key := make([]byte, shardPrefixSize())
	encodeShardPrefix(s.sid, shardId, key)
	value, err := shardInfo.Marshal()
	if err != nil {
		return err
	}

	if err := kvStore.SetRaw(ctx, dataCF, key, value, nil); err != nil {
		return err
	}*/

	shard, err := newShard(ctx, shardConfig{
		ShardBaseConfig: s.shardHandler.GetShardBaseConfig(),
		shardInfo:       *shardInfo,
		nodeInfo:        s.shardHandler.GetNodeInfo(),
		store:           s.store,
		raftManager:     s.shardHandler.GetRaftManager(),
	})
	if err != nil {
		return err
	}

	if err := shard.SaveShardInfo(ctx, false); err != nil {
		return err
	}

	s.shards.Store(shardId, shard)
	shard.Start()
	return nil
}

func (s *Space) UpdateShard(ctx context.Context, shardId uint32, epoch uint64) error {
	shard, err := s.GetShard(ctx, shardId)
	if err != nil {
		return err
	}

	return shard.UpdateShard(ctx, &persistent.ShardInfo{
		Epoch: epoch,
	})
}

func (s *Space) DeleteShard(ctx context.Context, shardId uint32) error {
	v, loaded := s.shards.LoadAndDelete(shardId)
	if !loaded {
		return nil
	}

	shard := v.(*shard)
	shard.Stop()
	shard.Close()
	// todo: clear shard's data

	return nil
}

func (s *Space) InsertItem(ctx context.Context, shardId uint32, item *proto.Item) (uint64, error) {
	if !s.validateFields(item.Fields) {
		return 0, apierrors.ErrUnknownField
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
		return apierrors.ErrUnknownField
	}

	shard := s.locateShard(ctx, item.Ino)
	if shard == nil {
		return apierrors.ErrInoRangeNotFound
	}

	return shard.UpdateItem(ctx, item)
}

func (s *Space) DeleteItem(ctx context.Context, ino uint64) error {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return apierrors.ErrInoRangeNotFound
	}

	return shard.DeleteItem(ctx, ino)
}

func (s *Space) GetItem(ctx context.Context, ino uint64) (*proto.Item, error) {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return nil, apierrors.ErrInoRangeNotFound
	}

	return shard.GetItem(ctx, ino)
}

func (s *Space) Link(ctx context.Context, link *proto.Link) error {
	shard := s.locateShard(ctx, link.Parent)
	if shard == nil {
		return apierrors.ErrInoRangeNotFound
	}

	return shard.Link(ctx, link)
}

func (s *Space) Unlink(ctx context.Context, unlink *proto.Unlink) error {
	shard := s.locateShard(ctx, unlink.Parent)
	if shard == nil {
		return apierrors.ErrInoRangeNotFound
	}

	return shard.Unlink(ctx, unlink)
}

func (s *Space) List(ctx context.Context, req *proto.ListRequest) ([]*proto.Link, error) {
	shard := s.locateShard(ctx, req.Ino)
	if shard == nil {
		return nil, apierrors.ErrInoRangeNotFound
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
		return nil, apierrors.ErrShardDoesNotExist
	}

	return v.(*shard), nil
}

func (s *Space) locateShard(ctx context.Context, ino uint64) *shard {
	shardId := uint32(ino/proto.ShardRangeStepSize + 1)
	s.shards.Range(func(key, value interface{}) bool {
		return true
	})
	v, ok := s.shards.Load(shardId)
	if !ok {
		return nil
	}
	return v.(*shard)

	/*found := s.shardsTree.Get(&shardRange{
		startIno: ino,
	})
	if found == nil {
		return nil
	}
	v, _ := s.shards.Load(found.(*shardRange).startIno / proto.ShardRangeStepSize)
	return v.(*shard)*/
}

func (s *Space) validateFields(fields []*proto.Field) bool {
	for _, field := range fields {
		if _, ok := s.fixedFields[field.Name]; !ok {
			return false
		}
	}
	return true
}

func calculateStartIno(shardId uint32) uint64 {
	return uint64(shardId-1)*proto.ShardRangeStepSize + 1
}
