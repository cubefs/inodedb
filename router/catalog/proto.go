package catalog

import "github.com/cubefs/inodedb/proto"

type ShardInfo struct {
	ShardId   uint32
	Epoch     uint64
	LeaderId  uint32
	SpaceName string
	Nodes     []uint32
}

type SpaceMeta struct {
	Sid    uint64
	Name   string
	Type   proto.SpaceType
	Shards []*ShardInfo
}
