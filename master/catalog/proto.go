package catalog

import (
	"encoding/binary"
	"encoding/json"

	"github.com/cubefs/inodedb/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	SpaceStatusInit        = SpaceStatus(1)
	SpaceStatusUpdateRoute = SpaceStatus(2)
	SpaceStatusNormal      = SpaceStatus(3)
)

const (
	SpaceExpandStatusNone        = uint8(0)
	SpaceExpandStatusUpdateRoute = uint8(1)
)

const (
	routerCF = "router"
	shardCF  = "shard"
)

// proto for storage encoding/decoding and function return value

type SpaceStatus uint8

type ChangeItem struct {
	RouteVersion uint64                  `json:"route_version"`
	Type         proto.CatalogChangeType `json:"type"`
	Item         *anypb.Any              `json:"item"`
}

func (c *ChangeItem) Key() []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key[4:], c.RouteVersion)
	return key
}

func (c *ChangeItem) Value() (value []byte, err error) {
	return json.Marshal(c)
}

type ChangeShardAdd struct {
	SpaceName    string                  `json:"space_name"`
	ShardId      uint32                  `json:"shard_id"`
	InoLimit     uint64                  `json:"ino_limit"`
	Replicates   []uint32                `json:"replicates"`
	RouteVersion uint64                  `json:"route_version"`
	Type         proto.CatalogChangeType `json:"type"`
}

type ChangeSpaceDelete struct {
	Sid          uint64                  `json:"sid"`
	Name         string                  `json:"name"`
	RouteVersion uint64                  `json:"route_version"`
	Type         proto.CatalogChangeType `json:"type"`
}

type ChangeSpaceAdd struct {
	Sid          uint64                  `json:"sid"`
	Name         string                  `json:"name"`
	Type         proto.SpaceType         `json:"type"`
	FixedFields  []*FieldMeta            `json:"fixed_fields"`
	RouteVersion uint64                  `json:"route_version"`
	ChangeType   proto.CatalogChangeType `json:"change_type"`
}

type FieldMeta struct {
	Name    string            `json:"name"`
	Type    proto.FieldType   `json:"type"`
	Indexed proto.IndexOption `json:"indexed"`
}

type spaceInfo struct {
	Sid             uint64          `json:"sid"`
	Name            string          `json:"name"`
	Type            proto.SpaceType `json:"type"`
	Status          SpaceStatus     `json:"status"`
	ExpandStatus    uint8           `json:"expand_status"`
	RouteVersion    uint64          `json:"route_version"`
	DesiredShardNum uint32          `json:"desired_shard_num"`
	CurrentShardId  uint32          `json:"current_shard_id"`
	FixedFields     []FieldMeta     `json:"fixed_fields"`
}

func (s *spaceInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *spaceInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type shardInfo struct {
	RouteVersion uint64   `json:"route_version"`
	ShardId      uint32   `json:"shard_id"`
	InoLimit     uint64   `json:"ino_limit"`
	InoUsed      uint64   `json:"ino_used"`
	Leader       uint32   `json:"leader"`
	Replicates   []uint32 `json:"replicates"`
}

func (s *shardInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *shardInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func protoFieldMetasToInternalFieldMetas(fields []*proto.FieldMeta) []FieldMeta {
	ret := make([]FieldMeta, len(fields))
	for i := range fields {
		ret[i] = FieldMeta{
			Name:    fields[i].Name,
			Type:    fields[i].Type,
			Indexed: fields[i].Indexed,
		}
	}
	return ret
}

func internalFieldMetasToProtoFieldMetas(fields []FieldMeta) []*proto.FieldMeta {
	ret := make([]*proto.FieldMeta, len(fields))
	for i := range fields {
		ret[i] = &proto.FieldMeta{
			Name:    fields[i].Name,
			Type:    fields[i].Type,
			Indexed: fields[i].Indexed,
		}
	}
	return ret
}
