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
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type Server struct {
	ctx context.Context

	catalog *Catalog
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

func (s *Server) CreateSpace(ctx context.Context, req *proto.CreateSpaceRequest) (*proto.CreateSpaceResponse, error) {
	// firstly, check if the space already created
	if s.catalog.exists(req.SpaceName) {
		err := errors.ErrSpaceAlreadyExists
		return nil, err
	}

	if err := s.catalog.Create(req.SpaceName); err != nil {
		return nil, err
	}
	return &proto.CreateSpaceResponse{}, nil
}

func (s *Server) DeleteSpace(ctx context.Context, req *proto.DeleteSpaceRequest) (*proto.DeleteSpaceResponse, error) {
	return &proto.DeleteSpaceResponse{}, nil
}

func (s *Server) UpsertItem(ctx context.Context, req *proto.UpsertItemRequest) (*proto.UpsertItemResponse, error) {
	return &proto.UpsertItemResponse{}, nil
}

func (s *Server) DeleteItem(ctx context.Context, req *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	return &proto.DeleteItemResponse{}, nil
}

func (s *Server) Search(ctx context.Context, req *proto.SearchRequest) (*proto.SearchResponse, error) {
	return &proto.SearchResponse{}, nil
}
