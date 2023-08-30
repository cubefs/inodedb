package catalog

import (
	"context"
	"google.golang.org/grpc"

	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type ShardServerClient interface {
	ShardInsertItem(ctx context.Context, in *proto.InsertItemRequest, opts ...grpc.CallOption) (*proto.InsertItemResponse, error)
	ShardUpdateItem(ctx context.Context, in *proto.UpdateItemRequest, opts ...grpc.CallOption) (*proto.UpdateItemResponse, error)
	ShardDeleteItem(ctx context.Context, in *proto.DeleteItemRequest, opts ...grpc.CallOption) (*proto.DeleteItemResponse, error)
	ShardGetItem(ctx context.Context, in *proto.GetItemRequest, opts ...grpc.CallOption) (*proto.GetItemResponse, error)
	ShardLink(ctx context.Context, in *proto.LinkRequest, opts ...grpc.CallOption) (*proto.LinkResponse, error)
	ShardUnlink(ctx context.Context, in *proto.UnlinkRequest, opts ...grpc.CallOption) (*proto.UnlinkResponse, error)
	ShardList(ctx context.Context, in *proto.ListRequest, opts ...grpc.CallOption) (*proto.ListResponse, error)
	ShardSearch(ctx context.Context, in *proto.SearchRequest, opts ...grpc.CallOption) (*proto.SearchResponse, error)
}

type Transporter interface {
	GetClient(ctx context.Context, nodeId uint32) (ShardServerClient, error)
	Close()
}

type ShardServerConfig struct {
	TransportConfig *sc.TransportConfig `json:"transport"`
	MasterClient    *sc.MasterClient    `json:"-"`
}

type transporter struct {
	shardServerClient *sc.ShardServerClient
}

var defaultClient Transporter

func NewShardServerClient(ctx context.Context, cfg *ShardServerConfig) error {
	serverClient, err := sc.NewShardServerClient(&sc.ShardServerConfig{
		MasterClient:    cfg.MasterClient,
		TransportConfig: *cfg.TransportConfig,
	})
	if err != nil {
		return err
	}
	defaultClient = &transporter{
		shardServerClient: serverClient,
	}
	return nil
}

func (c *transporter) GetClient(ctx context.Context, nodeId uint32) (ShardServerClient, error) {

	client, err := c.shardServerClient.GetClient(ctx, nodeId)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (c *transporter) Close() {
	if c.shardServerClient != nil {
		c.shardServerClient.Close()
	}
}
