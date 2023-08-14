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

package errors

import "errors"

// common error definition
var (
	ErrNotFound                 = errors.New("error not found")
	ErrCollectionDoesNotExist   = errors.New("the colleciton does not exist")
	ErrCollectionAlreadyCreated = errors.New("the collection is already created")

	ErrInoDoesNotExist = errors.New("ino does not exist")

	ErrNodeDoesNotFound = errors.New("node not found")

	ErrUnknownQueryType   = errors.New("unknown query type")
	ErrInvalidData        = errors.New("invalid data")
	ErrInvalidCredentials = errors.New("invalid credentials")

	ErrInvalidItem = errors.New("invalid document")

	ErrUnknownFieldType = errors.New("unknown field type")

	ErrUnknownIndexType = errors.New("unknown index type")
	ErrUnSupportedAPI   = errors.New("unsupported api")
)

// server error definition
var (
	ErrInodeLimitExceed      = errors.New("inode limit exceed")
	ErrInoOutOfRange         = errors.New("ino out of range")
	ErrSpaceDoesNotExist     = errors.New("space does do Exist")
	ErrShardDoesNotExist     = errors.New("shard does not exist")
	ErrInoRangeNotFound      = errors.New("ino range not found")
	ErrInoMismatchShardRange = errors.New("ino mismatch shard range")
	ErrListNumExceed         = errors.New("list num exceed")
	ErrInvalidShardID        = errors.New("invalid shard id")
	ErrUnknownField          = errors.New("unknown field")
)

// master error definition
var (
	ErrUnknownOperationType    = errors.New("unknown operation type")
	ErrNodeAlreadyExist        = errors.New("node has exist")
	ErrNodeNotExist            = errors.New("node not exist")
	ErrAzNotExist              = errors.New("az not exist")
	ErrNoAvailableNode         = errors.New("no available node")
	ErrCreateShardAlreadyExist = errors.New("create shard already exist")
	ErrRouteVersionConflict    = errors.New("route version conflict")
	ErrInvalidSpaceID          = errors.New("invalid space id")
	ErrSpaceNotExist           = errors.New("space not exist")
	ErrShardNotExist           = errors.New("space not exist")
)
