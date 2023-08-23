package catalog

import (
	"encoding/binary"
	"encoding/json"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

const (
	SpaceStateExpanding = SpaceState(0)
	SpaceStateNormal    = SpaceState(1)
)

const (
	routerCF = "router"
	shardCF  = "shard"
)

// proto for storage encoding/decoding and function return value

type SpaceState int

type spaceInfo struct {
	Sid         uint64          `json:"sid"`
	Name        string          `json:"name"`
	Type        proto.SpaceType `json:"type"`
	FixedFields []*FieldMeta    `json:"fixed_fields"`
}

func (s *spaceInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *spaceInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type shardInfo struct {
	RouteVersion uint64              `json:"route_version"`
	ShardId      uint32              `json:"shard_id"`
	InoLimit     uint64              `json:"ino_limit"`
	InoUsed      uint64              `json:"ino_used"`
	Replicates   []*cluster.NodeInfo `json:"replicates"`
}

func (s *shardInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *shardInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

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

type FieldMeta struct {
	Name    string
	Type    proto.FieldType
	Indexed proto.IndexOption
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

type ShardReport struct {
	SpaceID  uint64
	LeaderId uint32
	Info     *shardInfo
}

type ReportArgs struct {
	Id    uint32 // node id
	Infos []*ShardReport
}
