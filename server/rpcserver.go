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

	"github.com/cubefs/inodedb/proto"
)

type RPCServer struct{}

func (r *RPCServer) AddShard(ctx context.Context, req *proto.AddShardRequest) (*proto.AddShardResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetShard(context.Context, *proto.GetShardRequest) (*proto.GetShardResponse, error) {
	return nil, nil
}

func (r *RPCServer) InsertItem(context.Context, *proto.InsertItemRequest) (*proto.InsertItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) UpdateItem(context.Context, *proto.UpdateItemRequest) (*proto.UpdateItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) DeleteItem(context.Context, *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetItem(context.Context, *proto.GetItemRequest) (*proto.GetItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) Link(context.Context, *proto.LinkRequest) (*proto.LinkResponse, error) {
	return nil, nil
}

func (r *RPCServer) Unlink(context.Context, *proto.UnlinkRequest) (*proto.UnlinkResponse, error) {
	return nil, nil
}

func (r *RPCServer) List(context.Context, *proto.ListRequest) (*proto.ListResponse, error) {
	return nil, nil
}

func (r *RPCServer) Search(context.Context, *proto.SearchRequest) (*proto.SearchResponse, error) {
	return nil, nil
}
