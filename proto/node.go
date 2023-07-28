package proto

import (
	"encoding/json"
)

type NodeRole int

const (
	NodeRoleUnknown NodeRole = iota
	NodeRoleMaster
	NodeRoleRouter
	NodeRoleShardServer
)

type NodeInfo struct {
	Role     NodeRole `json:"role"`
	GrpcPort int      `json:"grpc_port"`
	HttpPort int      `json:"http_port"`
}
