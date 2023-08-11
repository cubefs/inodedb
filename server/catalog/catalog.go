package route

import (
	"context"
	"sync"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type Catalog struct {
	spaces sync.Map
}

func (c *Catalog) AddShard(ctx context.Context, spaceName string, shardID uint32, inoRange *proto.InoRange, replicates map[uint32]string) error {
	actual, _ := c.spaces.LoadOrStore(spaceName, &space{name: spaceName})
	space := actual.(*space)
	space.AddShard(ctx, shardID, inoRange, replicates)
	return nil
}

func (c *Catalog) GetShard(ctx context.Context, spaceName string, shardID uint32) (*proto.Shard, error) {
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		return nil, errors.ErrSpaceDoesNotExist
	}
	space := v.(*space)
	shard, err := space.GetShard(ctx, shardID)
	if err != nil {
		return nil, err
	}
	shardStat := shard.Stat()
	// TODO: transform replicates into nodes

	return &proto.Shard{
		ShardId:    shard.id,
		InoRange:   &shardStat.inoRange,
		InoUsed:    shardStat.inoUsed,
		Replicates: nil,
	}, nil
}

func (c *Catalog) InsertItem(ctx context.Context, spaceName string, shardID uint32, item *proto.Item) (uint64, error) {
	return 0, nil
}

func (c *Catalog) UpdateItem(ctx context.Context, spaceName string, item *proto.Item) error {
	return nil
}

func (c *Catalog) DeleteItem(ctx context.Context, spaceName string, ino uint64) error {
	return nil
}

func (c *Catalog) GetItem(ctx context.Context, spaceName string, ino uint64) (*proto.Item, error) {
	return nil, nil
}

func (c *Catalog) Link(ctx context.Context, spaceName string, link *proto.Link) error {
	return nil
}

func (c *Catalog) Unlink(ctx context.Context, spaceName string, unlink *proto.Unlink) error {
	return nil
}

func (c *Catalog) List(ctx context.Context, spaceName string, ino uint64, start string, num uint32) ([]*proto.Link, error) {
	return nil, nil
}

func (c *Catalog) Search(ctx context.Context) error {
	// todo
	return nil
}
