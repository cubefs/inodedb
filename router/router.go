package router

import (
	"context"

	"github.com/cubefs/inodedb/proto"

	"github.com/cubefs/inodedb/client"
)

type Config struct {
	MasterConfig client.MasterConfig `json:"master_config"`
	NodeConfig   proto.Node          `json:"node_config"`
}

type Router struct {
	ctx context.Context
	cli *client.MasterClient
}
