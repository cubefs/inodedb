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

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/raft"
)

const (
	RaftOpAlloc = iota + 1
)

const (
	module = "idGenerator"
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
	return module
}

func (s *idGenerator) Apply(ctx context.Context, op raft.Op, data []byte) error {
	switch op {
	case RaftOpAlloc:
		err := s.applyCommit(ctx, data)
		if err != nil {
			return errors.Info(err, "apply commit failed").Detail(err)
		}
	}

	return nil
}

func (s *idGenerator) Flush(ctx context.Context) error {
	return nil
}

func (s *idGenerator) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}
