package cluster

import (
	"encoding/json"

	"github.com/cubefs/inodedb/proto"
)

// proto for storage encoding/decoding and function return value

const (
	clusterCF       = "cluster"
	IDGeneratorName = "NODE"
)

type NodeInfo struct {
	Id          uint32           `json:"id"`
	Addr        string           `json:"addr"`
	GrpcPort    uint32           `json:"grpc_port"`
	HttpPort    uint32           `json:"http_port"`
	ReplicaPort uint32           `json:"replica_port"`
	Az          string           `json:"az"`
	Roles       []proto.NodeRole `json:"role"`
	State       proto.NodeState  `json:"state"`
}

func (s *NodeInfo) ToProtoNode() *proto.Node {
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
	}
}

func (s *NodeInfo) ToDBNode(node *proto.Node) {
	roles := make([]proto.NodeRole, len(s.Roles))
	copy(roles, node.Roles)

	s.Id = node.Id
	s.Addr = node.Addr
	s.GrpcPort = node.GrpcPort
	s.HttpPort = node.HttpPort
	s.Az = node.Az
	s.Roles = roles
	s.State = node.State
}

func (s *NodeInfo) Clone() *NodeInfo {
	info := &NodeInfo{}

	info.Id = s.Id
	info.Addr = s.Addr
	info.GrpcPort = s.GrpcPort
	info.HttpPort = s.HttpPort
	info.Az = s.Az
	info.State = s.State

	roles := make([]proto.NodeRole, len(s.Roles))
	copy(roles, s.Roles)
	info.Roles = roles

	return info
}

func (s *NodeInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *NodeInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type ShardAddArgs struct {
	NodeId       uint32            `json:"node_id"`
	Sid          *uint64           `json:"sid"`
	SpaceName    string            `json:"space_name"`
	ShardId      uint32            `json:"shard_id"`
	InoLimit     uint64            `json:"ino_limit"`
	Replicates   map[uint32]string `json:"replicates"`
	RouteVersion uint64            `json:"route_version"`
}

type AllocArgs struct {
	Count int            `json:"count"`
	Az    string         `json:"az"`
	Role  proto.NodeRole `json:"role"`
}

type HeartbeatArgs struct {
	NodeID     uint32 `json:"node_id"`
	ShardCount int32  `json:"shard_count"`
}
