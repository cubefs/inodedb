package proto

const (
	ShardRangeStepSize = uint64(2 << 40)
	ReqIdKey           = "req-id"
)

type InodeDB interface{}
