package proto

const (
	ShardRangeStepSize = uint64(2 << 40)
	ReqIdKey           = "req-id"
)

type (
	NodeID       = uint32
	Sid          = uint64
	DiskID       = uint32
	ShardID      = uint32
	RouteVersion = uint64
	SetID        = uint32
)
