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

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/master/store"
)

var (
	MaxCount = 1000000

	ErrInvalidCount = errors.New("request count is invalid")
)

// TODO: rename IDGenerator into IdGenerator

type IDGenerator interface {
	Alloc(ctx context.Context, name string, count int) (base, new uint64, err error)
}

type idGenerator struct {
	scopeItems map[string]uint64
	raftGroup  raft.Group

	storage *storage
	lock    sync.RWMutex
}

func NewIDGenerator(store *store.Store) (IDGenerator, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewIDGenerator")

	storage := &storage{kvStore: store.KVStore()}
	s := &idGenerator{storage: storage}
	if err := s.LoadData(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *idGenerator) SetRaftGroup(raftGroup raft.Group) {
	s.raftGroup = raftGroup
}

func (s *idGenerator) Alloc(ctx context.Context, name string, count int) (base, new uint64, err error) {
	if count <= 0 {
		return 0, 0, ErrInvalidCount
	}

	if count > MaxCount {
		count = MaxCount
	}

	// TODO: move scope increase in raft apply progress and return new scope by result
	s.lock.Lock()
	s.scopeItems[name] += uint64(count)
	new = s.scopeItems[name]
	s.lock.Unlock()

	args := &allocArgs{Name: name, Current: new}
	data, err := json.Marshal(args)
	if err != nil {
		return
	}

	if _, err = s.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpAlloc,
		Data:       data,
		WithResult: false,
	}); err != nil {
		return
	}

	// TODO: remove this function call after invoke raft
	if err = s.applyCommit(ctx, data); err != nil {
		return
	}

	base = new - uint64(count) + 1
	return
}

func (s *idGenerator) applyCommit(ctx context.Context, data []byte) error {
	args := &allocArgs{}
	err := json.Unmarshal(data, args)
	if err != nil {
		return errors.Info(err, "json unmarshal failed")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.scopeItems[args.Name] < args.Current {
		s.scopeItems[args.Name] = args.Current
	}

	current, err := s.storage.Get(ctx, args.Name)
	if err != nil && err != kvstore.ErrNotFound {
		return err
	}
	if current > args.Current {
		return nil
	}

	err = s.storage.Put(ctx, args.Name, args.Current)
	if err != nil {
		return err
	}

	return nil
}

type allocArgs struct {
	Name    string `json:"name"`
	Count   int    `json:"count"`
	Current uint64 `json:"current"`
}
