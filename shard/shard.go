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

package shard

import (
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type Shard struct {
	id     proto.ShardId
	cursor uint64
	endIno unt64

	store *Store
}

func (s *Shard) nextIno() (ino uint64, e error) {
	for {
		cur := atomic.LoadUint64(&s.cursor)
		if cur >= s.endIno {
			return 0, errors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.cursor, cur, newId) {
			return newId, nil
		}
	}
}
