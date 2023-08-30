package router

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/router/catalog"
)

type Config struct {
	MasterConfig *client.MasterConfig    `json:"master_config"`
	NodeConfig   *proto.Node             `json:"node_config"`
	ServerConfig *client.TransportConfig `json:"server_config"`
}

type Router struct {
	Catalog *catalog.Catalog
}

func NewRouter(cfg *Config) *Router {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	c := catalog.NewCatalog(ctx, &catalog.Config{
		ServerConfig: &catalog.ShardServerConfig{
			TransportConfig: cfg.ServerConfig,
		},
		MasterConfig: cfg.MasterConfig,
		NodeConfig:   cfg.NodeConfig,
	})
	return &Router{Catalog: c}
}
