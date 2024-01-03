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
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/raft"
)

const (
	RaftOpAlloc = iota + 1
)

var Module = []byte("idGenerator")

func (s *idGenerator) LoadData(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	scopeItems, err := s.storage.Load(ctx)
	if err != nil {
		return err
	}
	s.scopeItems = scopeItems
	span.Infof("scope item: %+v", scopeItems)
	return nil
}

func (s *idGenerator) Apply(ctx context.Context, pds []raft.ProposalData) (rets []interface{}, err error) {
	rets = make([]interface{}, 0, len(pds))
	for _, pd := range pds {
		data := pd.Data
		var ret interface{}
		span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "", string(pd.Context))
		span.Infof("recive raft operation, op %d", pd.Op)
		switch pd.Op {
		case RaftOpAlloc:
			ret, err = s.applyCommit(ctx, data)
		default:
			span.Errorf("unknown op, mod %s, op %d", pd.Module, pd.Op)
			return rets, errors.New(fmt.Sprintf("unsupported operation type: %d", pd.Op))
		}
		if err != nil {
			return nil, err
		}
		rets = append(rets, ret)
	}

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

	span.Debugf("alloc id success, name %s, current %d, new current %d", args.Name, current, newCurrent)
	return newCurrent, nil
}

// TODO:
func (s *idGenerator) LeaderChange(peerID uint64) error {
	return nil
}

func (s *idGenerator) Flush(ctx context.Context) error {
	return nil
}
