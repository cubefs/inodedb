package client

import (
	"context"

	shardServer "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

type Client interface {
	AddShard(ctx context.Context, args *cluster.ShardAddArgs) error
	Close()
}

type client struct {
	server *shardServer.ShardServerClient
}

func NewClient(ctx context.Context, cfg *shardServer.ShardServerConfig) (Client, error) {
	serverClient, err := shardServer.NewShardServerClient(cfg)
	if err != nil {
		return nil, err
	}
	return &client{
		server: serverClient,
	}, nil
}

func (c *client) AddShard(ctx context.Context, args *cluster.ShardAddArgs) error {
	addShardRequest := &proto.AddShardRequest{
		Sid:          args.Sid,
		ShardId:      args.ShardId,
		SpaceName:    args.SpaceName,
		RouteVersion: args.RouteVersion,
		InoLimit:     args.InoLimit,
		Replicates:   args.Replicates,
	}
	sc, err := c.server.GetClient(ctx, args.NodeId)
	if err != nil {
		return err
	}
	_, err = sc.AddShard(ctx, addShardRequest)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) Close() {
	c.server.Close()
}
