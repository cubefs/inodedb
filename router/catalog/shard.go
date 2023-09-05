package catalog

import (
	"context"

	"github.com/cubefs/inodedb/proto"
)

type shard struct {
	epoch   uint64
	shardId uint32

	info *ShardInfo
}

func (s *shard) InsertItem(ctx context.Context, req *proto.InsertItemRequest) (uint64, error) {
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
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
	leader := s.info.LeaderId
	if s.info.LeaderId == 0 {
		leader = s.info.Nodes[0]
	}
	sc, err := GetDefaultTransporter().GetClient(ctx, leader)
	if err != nil {
		return nil, err
	}
	resp, err := sc.ShardList(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Links, nil
}
