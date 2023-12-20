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
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/raft"
)

const (
	RaftOpAlloc = iota + 1
)

const (
	Module = "idGenerator"
)

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

func (s *idGenerator) GetModuleName() string {
	return Module
}

func (s *idGenerator) Apply(cxt context.Context, pd raft.ProposalData, index uint64) (rets interface{}, err error) {
	data := pd.Data
	span, ctx := trace.StartSpanFromContextWithTraceID(cxt, "", string(pd.Context))
	span.Infof("recive raft operation, op %d", pd.Op)
	switch pd.Op {
	case RaftOpAlloc:
		return s.applyCommit(ctx, data)
	default:
		return rets, errors.New(fmt.Sprintf("unsupported operation type: %d", pd.Op))
	}
}

// TODO:
func (s *idGenerator) LeaderChange(peerID uint64) error {
	return nil
}

func (s *idGenerator) Flush(ctx context.Context) error {
	return nil
}
