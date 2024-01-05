// Copyright 2022 The CubeFS Authors.
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

package idgenerator

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/master/base"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/raft"
)

var (
	MaxCount = 1000000

	ErrInvalidCount = errors.New("request count is invalid")
)

type IDGenerator interface {
	Alloc(ctx context.Context, name string, count int) (base, new uint64, err error)
	SetRaftGroup(raftGroup base.RaftServer)
	GetCF() []kvstore.CF
	GetModule() string
}

type idGenerator struct {
	scopeItems map[string]uint64
	raftGroup  base.RaftServer

	storage *storage
	lock    sync.RWMutex
}

func NewIDGenerator(store *store.Store, raftGroup base.RaftServer) (IDGenerator, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewIDGenerator")

	storage := &storage{kvStore: store.KVStore()}
	s := &idGenerator{storage: storage}
	s.raftGroup = raftGroup
	if err := s.LoadData(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *idGenerator) GetCF() []kvstore.CF {
	return CFs
}

func (s *idGenerator) GetModule() string {
	return string(Module)
}

func (s *idGenerator) SetRaftGroup(raftGroup base.RaftServer) {
	s.raftGroup = raftGroup
}

func (s *idGenerator) Alloc(ctx context.Context, name string, count int) (base, new uint64, err error) {
	span := trace.SpanFromContext(ctx)
	if count <= 0 {
		return 0, 0, ErrInvalidCount
	}

	if count > MaxCount {
		count = MaxCount
	}

	args := &allocArgs{Name: name, Count: count}
	data, err := json.Marshal(args)
	if err != nil {
		return
	}

	ret, err := s.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: Module,
		Op:     RaftOpAlloc,
		Data:   data,
	})
	if err != nil {
		span.Errorf("propose failed, name %s, err %v", name, err)
		return
	}

	new = ret.Data.(uint64)
	base = new - uint64(count) + 1
	span.Debugf("alloc success, name %s, base %d, new %d", name, base, new)
	return
}

type allocArgs struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}
