package proto

import "math"

const ReqIdKey = "req-id"

const (
	ShardRangeStepSize = uint64(2 << 40)
	MaxShardNum        = math.MaxUint64 / ShardRangeStepSize
	MaxSpaceNum        = (math.MaxUint64 - MaxShardNum) / MaxShardNum
)

type (
	NodeID       = uint32
	Sid          = uint64
	DiskID       = uint32
	ShardID      = uint32
	RouteVersion = uint64
	SetID        = uint32
)
