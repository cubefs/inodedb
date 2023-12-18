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
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/raft"
)

var (
	MaxCount = 1000000

	ErrInvalidCount = errors.New("request count is invalid")
)

// TODO: rename IDGenerator into IdGenerator

type IDGenerator interface {
	Alloc(ctx context.Context, name string, count int) (base, new uint64, err error)
	GetSM() raft.Applier
}

type idGenerator struct {
	scopeItems map[string]uint64
	raftGroup  raft.Group

	storage *storage
	lock    sync.RWMutex
	raft.StateMachine
}

func NewIDGenerator(store *store.Store, raftGroup raft.Group) (IDGenerator, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewIDGenerator")

	storage := &storage{kvStore: store.KVStore()}
	s := &idGenerator{storage: storage}
	s.raftGroup = raftGroup
	if err := s.LoadData(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *idGenerator) GetSM() raft.Applier {
	return s
}

func (s *idGenerator) SetRaftGroup(raftGroup raft.Group) {
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

	args := &allocArgs{Name: name, Current: new, Count: count}
	data, err := json.Marshal(args)
	if err != nil {
		return
	}

	ret, err := s.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpAlloc,
		Data:   data,
	})

	if err != nil {
		span.Errorf("propose failed, name %s, err %v", name, err)
		return
	}

	base = ret.Data.(uint64)
	new = base + uint64(count)
	span.Debugf("alloc success, name %s, base %d, new %d", name, base, new)
	return
}

func (s *idGenerator) applyCommit(ctx context.Context, data []byte) (uint64, error) {
	span := trace.SpanFromContext(ctx)
	args := &allocArgs{}
	err := json.Unmarshal(data, args)
	if err != nil {
		return 0, errors.Info(err, "json unmarshal failed")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	current, err := s.storage.Get(ctx, args.Name)
	if err != nil && err != kvstore.ErrNotFound {
		return 0, err
	}

	newCurrent := current + uint64(args.Count)
	err = s.storage.Put(ctx, args.Name, newCurrent)
	if err != nil {
		span.Errorf("put id failed, name %s, err: %v", args.Name, err)
		return 0, err
	}

	span.Debugf("put id success, name %s, current %d, new current %d", args.Name, current, newCurrent)
	return newCurrent, nil
}

type allocArgs struct {
	Name    string `json:"name"`
	Count   int    `json:"count"`
	Current uint64 `json:"current"`
}
