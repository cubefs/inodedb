package catalog

import (
	"encoding/binary"
	"encoding/json"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

// proto for storage encoding/decoding and function return value

type CatalogInfo struct {
	Name       string   `json:"name"`
	CreateTime int64    `json:"create_time"`
	SpaceIDs   []uint64 `json:"space_id_s"`
}

type SpaceState int

const (
	SpaceStateExpanding = SpaceState(0)
	SpaceStateNormal    = SpaceState(1)
)
const (
	routerCF = "router"
	shardCF  = "shard"
)

func (c *CatalogInfo) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *CatalogInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

type SpaceInfo struct {
	Sid         uint64          `json:"sid"`
	Name        string          `json:"name"`
	Type        proto.SpaceType `json:"type"`
	FixedFields []*FieldMeta    `json:"fixed_fields"`
	ShardIDs    []*uint32       `json:"shard_id_s"`
}

func (s *SpaceInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *SpaceInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type ShardInfo struct {
	RouteVersion uint64              `json:"route_version"`
	ShardId      uint32              `json:"shard_id"`
	InoLimit     uint64              `json:"ino_limit"`
	InoUsed      uint64              `json:"ino_used"`
	Replicates   []*cluster.NodeInfo `json:"replicates"`
}

func (s *ShardInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *ShardInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *ShardInfo) Equal(info *ShardInfo) bool {

	return false
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

type HeartBeatArgs struct {
	Id uint32 `json:"id"` // node id
}

type ShardReport struct {
	SpaceID  uint64
	LeaderId uint32
	Info     *ShardInfo
}

type ReportArgs struct {
	Id    uint32 // node id
	Infos []*ShardReport
}
