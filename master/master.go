package master

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/master/catalog"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
)

type Config struct {
	StoreConfig   store.Config   `json:"store_config"`
	CatalogConfig catalog.Config `json:"catalog_config"`
	ClusterConfig cluster.Config `json:"cluster_config"`
}

type Master struct {
	catalog.Catalog
	cluster.Cluster
}

func NewMaster(cfg *Config) *Master {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Fatalf("new store failed: %s", err)
	}
	newCluster := cluster.NewCluster(ctx, &cfg.ClusterConfig)

	idGenerator, err := idgenerator.NewIDGenerator(store)
	if err != nil {
		span.Fatalf("new id generator failed: %s", err)
	}

	cfg.CatalogConfig.IdGenerator = idGenerator
	cfg.CatalogConfig.Store = store
	cfg.ClusterConfig.Store = store
	cfg.ClusterConfig.IdGenerator = idGenerator
	cfg.CatalogConfig.Cluster = newCluster

	return &Master{
		Catalog: catalog.NewCatalog(ctx, &cfg.CatalogConfig),
		Cluster: newCluster,
	}
}
