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

package proto

import "errors"

var (
	ErrCollectionDoesNotExist   = errors.New("the colleciton does not exist")
	ErrCollectionAlreadyCreated = errors.New("the collection is already created")

	ErrShardDoesNotExist = errors.New("shard does not exist")

	ErrInoDoesNotExist = errors.New("ino does not exist")

	ErrNodeDoesNotFound = errors.New("node not found")

	ErrUnknownQueryType   = errors.New("unknown query type")
	ErrInvalidData        = errors.New("invalid data")
	ErrInvalidCredentials = errors.New("invalid credentials")

	ErrInvalidItem = errors.New("invalid document")

	ErrUnknownFieldType = errors.New("unknown field type")

	ErrUnknownIndexType = errors.New("unknown index type")
)
