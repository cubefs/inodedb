// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type RPCServer struct {
	*Server
}

func (r *RPCServer) AddShard(ctx context.Context, req *proto.AddShardRequest) (*proto.AddShardResponse, error) {
	shardServer := r.shardServer
	err := shardServer.Catalog.AddShard(ctx, req.SpaceName, req.ShardId, req.RouteVersion, req.InoLimit, req.Replicates)
	return nil, err
}

func (r *RPCServer) GetShard(ctx context.Context, req *proto.GetShardRequest) (*proto.GetShardResponse, error) {
	shardServer := r.shardServer
	shard, err := shardServer.Catalog.GetShard(ctx, req.SpaceName, req.ShardId)
	if err != nil {
		return nil, err
	}

	return &proto.GetShardResponse{Shard: shard}, nil
}

func (r *RPCServer) InsertItem(ctx context.Context, req *proto.InsertItemRequest) (*proto.InsertItemResponse, error) {
	if req.PreferredShard == 0 {
		return nil, errors.ErrInvalidShardID
	}
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	ino, err := space.InsertItem(ctx, req.PreferredShard, req.Item)
	if err != nil {
		return nil, err
	}

	return &proto.InsertItemResponse{Ino: ino}, nil
}

func (r *RPCServer) UpdateItem(ctx context.Context, req *proto.UpdateItemRequest) (*proto.UpdateItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.UpdateItem(ctx, req.Item)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) DeleteItem(ctx context.Context, req *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.DeleteItem(ctx, req.Ino)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) GetItem(ctx context.Context, req *proto.GetItemRequest) (*proto.GetItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	item, err := space.GetItem(ctx, req.Ino)
	if err != nil {
		return nil, err
	}
	return &proto.GetItemResponse{Item: item}, nil
}

func (r *RPCServer) Link(ctx context.Context, req *proto.LinkRequest) (*proto.LinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.Link(ctx, req.Link)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) Unlink(ctx context.Context, req *proto.UnlinkRequest) (*proto.UnlinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.Unlink(ctx, req.Unlink)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) List(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	if req.Num > maxListNum {
		return nil, errors.ErrListNumExceed
	}
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	links, err := space.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return &proto.ListResponse{Links: links}, nil
}

func (r *RPCServer) Search(context.Context, *proto.SearchRequest) (*proto.SearchResponse, error) {
	return nil, nil
}

func (r *RPCServer) Cluster(context.Context, *proto.ClusterRequest) (*proto.ClusterResponse, error) {
	return nil, nil
}

func (r *RPCServer) CreateSpace(context.Context, *proto.CreateSpaceRequest) (*proto.CreateSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) DeleteSpace(context.Context, *proto.DeleteSpaceRequest) (*proto.DeleteSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetSpace(context.Context, *proto.GetSpaceRequest) (*proto.GetSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) Heartbeat(context.Context, *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	return nil, nil
}

func (r *RPCServer) Report(context.Context, *proto.ReportRequest) (*proto.ReportResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetNode(context.Context, *proto.GetNodeRequest) (*proto.GetNodeResponse, error) {
	return nil, nil
}
