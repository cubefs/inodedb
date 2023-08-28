package client

import (
	"context"

	"google.golang.org/grpc"

	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type MasterClient interface {
	AddShard(ctx context.Context, in *proto.AddShardRequest, opts ...grpc.CallOption) (*proto.AddShardResponse, error)
	ShardList(ctx context.Context, in *proto.ListRequest, opts ...grpc.CallOption) (*proto.ListResponse, error)
	GetShard(ctx context.Context, in *proto.GetShardRequest, opts ...grpc.CallOption) (*proto.GetShardResponse, error)
	UpdateShard(ctx context.Context, in *proto.UpdateShardRequest, opts ...grpc.CallOption) (*proto.UpdateShardResponse, error)
}

type Client interface {
	GetClient(ctx context.Context, nodeId uint32) (MasterClient, error)
	Close()
}

type Config struct {
	sc.ShardServerConfig
}

type clientMgr struct {
	shardServerClient *sc.ShardServerClient
}

func NewClient(ctx context.Context, cfg *Config) (Client, error) {
	serverClient, err := sc.NewShardServerClient(&cfg.ShardServerConfig)
	if err != nil {
		return nil, err
	}
	return &clientMgr{
		shardServerClient: serverClient,
	}, nil
}

func (c *clientMgr) GetClient(ctx context.Context, nodeId uint32) (MasterClient, error) {
	return c.shardServerClient.GetClient(ctx, nodeId)
}

func (c *clientMgr) Close() {
	if c.shardServerClient != nil {
		c.shardServerClient.Close()
	}
}
