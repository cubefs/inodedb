package cluster

import (
	"encoding/json"

	"github.com/cubefs/inodedb/proto"
)

// proto for storage encoding/decoding and function return value

const (
	nodeCF = "node"
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
	return &proto.Node{
		Id:       s.Id,
		Addr:     s.Addr,
		GrpcPort: s.GrpcPort,
		HttpPort: s.HttpPort,
		Az:       s.Az,
		Roles:    s.Roles,
		State:    s.State,
	}
}

func (s *NodeInfo) ToDBNode(node *proto.Node) {
	s.Id = node.Id
	s.Addr = node.Addr
	s.GrpcPort = node.GrpcPort
	s.HttpPort = node.HttpPort
	s.Az = node.Az
	s.Roles = node.Roles
	s.State = node.State
}

func (s *NodeInfo) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *NodeInfo) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

type AllocArgs struct {
	Count int    `json:"count"`
	Az    string `json:"az"`
}
