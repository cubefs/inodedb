package master

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/common/raft"
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

	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, catalog.CF, cluster.CF, idgenerator.CF)
	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Fatalf("new store failed: %s", err)
	}
	raftGroup := raft.NewRaftGroup(&raft.Config{
		Raft: &raft.NoopRaft{},
	})

	idGenerator, err := idgenerator.NewIDGenerator(store, raftGroup)
	if err != nil {
		span.Fatalf("new id generator failed: %s", err)
	}

	cfg.CatalogConfig.IdGenerator = idGenerator
	cfg.CatalogConfig.Store = store
	cfg.ClusterConfig.Store = store
	cfg.ClusterConfig.IdGenerator = idGenerator
	cfg.ClusterConfig.RaftGroup = raftGroup
	cfg.CatalogConfig.RaftGroup = raftGroup

	newCluster := cluster.NewCluster(ctx, &cfg.ClusterConfig)
	cfg.CatalogConfig.Cluster = newCluster
	return &Master{
		Catalog: catalog.NewCatalog(ctx, &cfg.CatalogConfig),
		Cluster: newCluster,
	}
}
