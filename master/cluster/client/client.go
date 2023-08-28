package client

import (
	"context"

	"google.golang.org/grpc"

	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type Client interface {
	AddShard(ctx context.Context, in *proto.AddShardRequest, opts ...grpc.CallOption) (*proto.AddShardResponse, error)
	ShardList(ctx context.Context, in *proto.ListRequest, opts ...grpc.CallOption) (*proto.ListResponse, error)
	GetShard(ctx context.Context, in *proto.GetShardRequest, opts ...grpc.CallOption) (*proto.GetShardResponse, error)
	UpdateShard(ctx context.Context, in *proto.UpdateShardRequest, opts ...grpc.CallOption) (*proto.UpdateShardResponse, error)
}

type clientMgr struct {
	server *sc.ShardServerClient
}

type ClientMgr interface {
	GetClient(ctx context.Context, nodeId uint32) (Client, error)
	Close()
}

type Config struct {
	sc.ShardServerConfig
}

func NewClient(ctx context.Context, cfg *Config) (ClientMgr, error) {
	serverClient, err := sc.NewShardServerClient(&cfg.ShardServerConfig)
	if err != nil {
		return nil, err
	}
	return &clientMgr{
		server: serverClient,
	}, nil
}

func (c *clientMgr) GetClient(ctx context.Context, nodeId uint32) (Client, error) {
	return c.server.GetClient(ctx, nodeId)
}

func (c *clientMgr) Close() {
	c.server.Close()
}
