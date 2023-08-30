package shardserver

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/catalog"
	"github.com/cubefs/inodedb/shardserver/store"
)

type Config struct {
	StoreConfig  store.Config        `json:"store_config"`
	MasterConfig client.MasterConfig `json:"master_config"`
	NodeConfig   proto.Node          `json:"node_config"`
}

type ShardServer struct {
	*catalog.Catalog
}

func NewShardServer(cfg *Config) *ShardServer {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	catalog := catalog.NewCatalog(ctx, &catalog.Config{
		StoreConfig:  cfg.StoreConfig,
		MasterConfig: cfg.MasterConfig,
		NodeConfig:   cfg.NodeConfig,
	})
	return &ShardServer{Catalog: catalog}
}
