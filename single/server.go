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

package single

import (
	"context"
	"github.com/cbuefs/inoder/errors"
	"github.com/cubefs/inoder/proto"
)

type Server struct {
	ctx context.Context

	collectionStore *CollectionStore
	shardStore      *ShardStore
}

func NewServer() (*Server, error) {
	return nil, nil
}

func (s *Server) Start() error {
	return nil
}

func (s *Server) Stop() error {
	return nil
}

func (s *Server) CreateCollection(ctx context.Context, req *proto.CreateCollectionRequest) (*proto.CreateCollectionResponse, error) {
	// firstly, check if the colleciton already created
	if s.collectionStore.Exists(req.CollectionName) {
		err := errors.ErrCollectionAlreadyExists
		return nil, err
	}

	//then insert a record into collectionStore
	if err := s.collecitonStore.Insert(req.CollectionName); err != nil {
		return nil, err
	}
	return &proto.CreateCollectionResponse{}, nil
}

func (s *Server) DeleteCollection(ctx context.Context, req *proto.DeleteCollectionRequest) (*proto.DeleteCollectionResponse, error) {

	return &proto.DeleteCollectionResponse{}, nil
}

func (s *Server) AddItems(ctx context.Context, req *proto.AddItemsRequest) (*proto.AddItemsResponse, error) {
	return &proto.AddDocumentsResponse{}, nil
}

func (s *Server) DelItems(ctx context.Context, req *proto.DelItemsRequest) (*proto.DelItemsResponse, error) {
	return &proto.DelItemsResponse{}, nil
}

func (s *Server) Search(ctx context.Context, req *proto.SearchRequest) (*proto.SearchResponse, error) {
	return &proto.SearchResponse{}, nil
}
