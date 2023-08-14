package client

import (
	"context"

	"github.com/cubefs/inodedb/proto"
)

type Client interface {
	AddShard(context.Context, string, *proto.AddShardRequest) (*proto.AddShardResponse, error)
	Close()
}

type client struct {
}
