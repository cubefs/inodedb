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
	GetShard(ctx context.Context, in *proto.GetShardRequest, opts ...grpc.CallOption) (*proto.GetShardResponse, error)
	ShardInsertItem(ctx context.Context, in *proto.ShardInsertItemRequest, opts ...grpc.CallOption) (*proto.ShardInsertItemResponse, error)
	ShardUpdateItem(ctx context.Context, in *proto.ShardUpdateItemRequest, opts ...grpc.CallOption) (*proto.ShardUpdateItemResponse, error)
	ShardDeleteItem(ctx context.Context, in *proto.ShardDeleteItemRequest, opts ...grpc.CallOption) (*proto.ShardDeleteItemResponse, error)
	ShardGetItem(ctx context.Context, in *proto.ShardGetItemRequest, opts ...grpc.CallOption) (*proto.ShardGetItemResponse, error)
	ShardLink(ctx context.Context, in *proto.ShardLinkRequest, opts ...grpc.CallOption) (*proto.ShardLinkResponse, error)
	ShardUnlink(ctx context.Context, in *proto.ShardUnlinkRequest, opts ...grpc.CallOption) (*proto.ShardUnlinkResponse, error)
	ShardList(ctx context.Context, in *proto.ShardListRequest, opts ...grpc.CallOption) (*proto.ShardListResponse, error)
	ShardSearch(ctx context.Context, in *proto.ShardSearchRequest, opts ...grpc.CallOption) (*proto.ShardSearchResponse, error)
}

type Transport interface {
	GetShardServerClient(ctx context.Context, nodeId uint32) (ShardServerClient, error)
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

func (c *transport) GetShardServerClient(ctx context.Context, diskId uint32) (ShardServerClient, error) {
	return c.shardServerClient.GetShardServerClient(ctx, diskId)
}

func (c *transport) Close() {
	if c.shardServerClient != nil {
		c.shardServerClient.Close()
	}
}
