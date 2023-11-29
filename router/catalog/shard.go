package catalog

import (
	"context"

	"github.com/cubefs/inodedb/proto"
)

type ShardInfo struct {
	ShardID proto.ShardID
	Epoch   uint64
	Leader  proto.DiskID
	Nodes   []proto.ShardNode
}

type shard struct {
	shardID uint32
	epoch   uint64
	tr      *transport

	info *ShardInfo
}

func (s *shard) InsertItem(ctx context.Context, req *proto.ShardInsertItemRequest) (uint64, error) {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return 0, err
	}
	item, err := sc.ShardInsertItem(ctx, req)
	if err != nil {
		return 0, err
	}
	return item.Ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, req *proto.ShardUpdateItemRequest) error {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return err
	}
	_, err = sc.ShardUpdateItem(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) DeleteItem(ctx context.Context, req *proto.ShardDeleteItemRequest) error {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return err
	}
	_, err = sc.ShardDeleteItem(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) GetItem(ctx context.Context, req *proto.ShardGetItemRequest) (proto.Item, error) {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return proto.Item{}, err
	}
	resp, err := sc.ShardGetItem(ctx, req)
	if err != nil {
		return proto.Item{}, err
	}
	return resp.Item, nil
}

func (s *shard) Link(ctx context.Context, req *proto.ShardLinkRequest) error {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return err
	}
	_, err = sc.ShardLink(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) Unlink(ctx context.Context, req *proto.ShardUnlinkRequest) error {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return err
	}
	_, err = sc.ShardUnlink(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) List(ctx context.Context, req *proto.ShardListRequest) (ret []proto.Link, err error) {
	sc, err := s.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := sc.ShardList(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Links, nil
}

func (s *shard) getLeaderClient(ctx context.Context) (ShardServerClient, error) {
	leader := s.info.Leader
	if s.info.Leader == 0 {
		leader = s.info.Nodes[0].DiskID
	}
	return s.tr.GetClient(ctx, leader)
}
