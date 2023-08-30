package catalog

import (
	"context"
	"github.com/cubefs/inodedb/proto"
)

type shard struct {
	routeVersion uint64
	shardId      uint32

	info *ShardInfo
}

func (s *shard) InsertItem(ctx context.Context, req *proto.InsertItemRequest) (uint64, error) {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return 0, err
	}
	item, err := sc.ShardInsertItem(ctx, req)
	if err != nil {
		return 0, err
	}
	return item.Ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, req *proto.UpdateItemRequest) error {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return err
	}
	_, err = sc.ShardUpdateItem(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) DeleteItem(ctx context.Context, req *proto.DeleteItemRequest) error {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return err
	}
	_, err = sc.ShardDeleteItem(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) GetItem(ctx context.Context, req *proto.GetItemRequest) (*proto.Item, error) {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return nil, err
	}
	resp, err := sc.ShardGetItem(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Item, nil
}

func (s *shard) Link(ctx context.Context, req *proto.LinkRequest) error {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return err
	}
	_, err = sc.ShardLink(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) Unlink(ctx context.Context, req *proto.UnlinkRequest) error {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return err
	}
	_, err = sc.ShardUnlink(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (s *shard) List(ctx context.Context, req *proto.ListRequest) (ret []*proto.Link, err error) {
	sc, err := defaultClient.GetClient(ctx, s.info.LeaderId)
	if err != nil {
		return nil, err
	}
	resp, err := sc.ShardList(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Links, nil
}