package cluster

import (
	"encoding/json"

	"github.com/cubefs/inodedb/proto"
)

// proto for set encoding/decoding and function return value

const (
	clusterCF  = "cluster"
	nodeIdName = "node"
)

type nodeInfo struct {
	Id          uint32           `json:"id"`
	Addr        string           `json:"addr"`
	GrpcPort    uint32           `json:"grpc_port"`
	HttpPort    uint32           `json:"http_port"`
	ReplicaPort uint32           `json:"replica_port"`
	Az          string           `json:"az"`
	Rack        string           `json:"rack"`
	Roles       []proto.NodeRole `json:"roles"`
	State       proto.NodeState  `json:"state"`
}

func (s *nodeInfo) CompareRoles(roles []proto.NodeRole) bool {
	if len(s.Roles) != len(roles) {
		return false
	}
	mp := make(map[proto.NodeRole]bool, len(s.Roles))
	for _, role := range s.Roles {
		mp[role] = true
	}
	for _, role := range roles {
		if !mp[role] {
			return false
		}
	}
	return true
}

func (s *nodeInfo) UpdateRoles(roles []proto.NodeRole) {
	s.Roles = roles
}

func (s *nodeInfo) ToProtoNode() *proto.Node {
	roles := make([]proto.NodeRole, len(s.Roles))
	copy(roles, s.Roles)

	return &proto.Node{
		Id:       s.Id,
		Addr:     s.Addr,
		GrpcPort: s.GrpcPort,
		HttpPort: s.HttpPort,
		Az:       s.Az,
		Roles:    roles,
		State:    s.State,
		Rack:     s.Rack,
	}
}

func (s *nodeInfo) ToDBNode(node *proto.Node) {
	roles := make([]proto.NodeRole, len(node.Roles))
	copy(roles, node.Roles)

	s.Id = node.Id
	s.Addr = node.Addr
	s.GrpcPort = node.GrpcPort
	s.HttpPort = node.HttpPort
	s.Az = node.Az
	s.Roles = roles
	s.Rack = node.Rack
	s.State = node.State
}

func (s *nodeInfo) Clone() *nodeInfo {
	info := &nodeInfo{}

	info.Id = s.Id
	info.Addr = s.Addr
	info.GrpcPort = s.GrpcPort
	info.HttpPort = s.HttpPort
	info.Az = s.Az
	info.State = s.State
	info.Rack = s.Rack

	roles := make([]proto.NodeRole, len(s.Roles))
	copy(roles, s.Roles)
	info.Roles = roles

	return info
}

func (s *nodeInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *nodeInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type AllocArgs struct {
	Count          int            `json:"count"`
	AZ             string         `json:"az"`
	Role           proto.NodeRole `json:"role"`
	RackWare       bool           `json:"rack_ware"`
	ExcludeNodeIds []uint32       `json:"exclude_node_ids"`
}

type HeartbeatArgs struct {
	NodeID     uint32 `json:"node_id"`
	ShardCount int32  `json:"shard_count"`
}
