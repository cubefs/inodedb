package transport

import (
	"context"
	"strconv"

	"google.golang.org/grpc"

	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/util"
)

type ShardServerClient interface {
	AddShard(ctx context.Context, in *proto.AddShardRequest, opts ...grpc.CallOption) (*proto.AddShardResponse, error)
	UpdateShard(ctx context.Context, in *proto.UpdateShardRequest, opts ...grpc.CallOption) (*proto.UpdateShardResponse, error)
}

type Transport interface {
	GetShardServerClient(ctx context.Context, diskID proto.DiskID) (ShardServerClient, error)
	Close()
}

type Config struct {
	GrpcPort        uint32             `json:"-"`
	TransportConfig sc.TransportConfig `json:"transport_config"`
}

type transport struct {
	shardServerClient *sc.ShardServerClient
}

func NewTransport(ctx context.Context, cfg *Config) (Transport, error) {
	localIP, err := util.GetLocalIP()
	if err != nil {
		return nil, err
	}
	serverClient, err := sc.NewShardServerClient(&sc.ShardServerConfig{
		MasterAddresses: localIP + ":" + strconv.Itoa(int(cfg.GrpcPort)),
		TransportConfig: cfg.TransportConfig,
	})
	if err != nil {
		return nil, err
	}
	return &transport{
		shardServerClient: serverClient,
	}, nil
}

func (c *transport) GetShardServerClient(ctx context.Context, diskID proto.DiskID) (ShardServerClient, error) {
	return c.shardServerClient.GetShardServerClient(ctx, diskID)
}

func (c *transport) Close() {
	if c.shardServerClient != nil {
		c.shardServerClient.Close()
	}
}
