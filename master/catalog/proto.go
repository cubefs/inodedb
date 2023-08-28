package catalog

import (
	"encoding/json"
	"errors"

	"github.com/cubefs/inodedb/proto"
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

type routeItemInfo struct {
	RouteVersion uint64                  `json:"route_version"`
	Type         proto.CatalogChangeType `json:"type"`
	ItemDetail   interface{}             `json:"item"`
}

func (r *routeItemInfo) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *routeItemInfo) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, r); err != nil {
		return err
	}
	switch r.Type {
	case proto.CatalogChangeType_AddSpace:
		r.ItemDetail = &routeItemSpaceAdd{}
		return json.Unmarshal(data, r)
	case proto.CatalogChangeType_DeleteSpace:
		r.ItemDetail = &routeItemSpaceDelete{}
		return json.Unmarshal(data, r)
	case proto.CatalogChangeType_AddShard:
		r.ItemDetail = &routeItemShardAdd{}
		return json.Unmarshal(data, r)
	}
	return errors.New("unsupported route item type")
}

type routeItemShardAdd struct {
	Sid     uint64 `json:"sid"`
	ShardId uint32 `json:"shard_id"`
}

type routeItemSpaceDelete struct {
	Sid uint64 `json:"sid"`
}

type routeItemSpaceAdd struct {
	Sid uint64 `json:"sid"`
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
	Epoch           uint64          `json:"epoch"`
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
	ShardId    uint32   `json:"shard_id"`
	Epoch      uint64   `json:"epoch"`
	InoLimit   uint64   `json:"ino_limit"`
	InoUsed    uint64   `json:"ino_used"`
	Leader     uint32   `json:"leader"`
	Replicates []uint32 `json:"replicates"`
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
