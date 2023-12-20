package shardserver

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/shardserver/catalog"
)

type Config struct {
	CatalogConfig catalog.Config `json:"catalog_config"`
}

type ShardServer struct {
	*catalog.Catalog
}

func NewShardServer(cfg *Config) *ShardServer {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	catalog := catalog.NewCatalog(ctx, &cfg.CatalogConfig)
	return &ShardServer{Catalog: catalog}
}
