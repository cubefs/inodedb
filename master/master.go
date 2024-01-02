package master

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/master/catalog"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/raft"
)

type Config struct {
	StoreConfig   store.Config   `json:"store_config"`
	CatalogConfig catalog.Config `json:"catalog_config"`
	ClusterConfig cluster.Config `json:"cluster_config"`
	RaftConfig    RaftNodeCfg    `json:"raft_config"`
}

type Master struct {
	catalog.Catalog
	cluster.Cluster
}

func NewMaster(cfg *Config) *Master {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	initConfig(cfg)
	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Fatalf("new store failed: %s", err)
	}

	idGenerator, err := idgenerator.NewIDGenerator(store, nil)
	if err != nil {
		span.Fatalf("new id generator failed: %s", err)
	}

	cfg.ClusterConfig.Store = store
	cfg.ClusterConfig.IdGenerator = idGenerator
	newCluster := cluster.NewCluster(ctx, &cfg.ClusterConfig)

	cfg.CatalogConfig.IdGenerator = idGenerator
	cfg.CatalogConfig.Store = store
	cfg.CatalogConfig.Cluster = newCluster
	newCatalog := catalog.NewCatalog(ctx, &cfg.CatalogConfig)

	raftNode := newRaftNode(ctx, &cfg.RaftConfig, store)
	if err != nil {
		span.Fatalf("new raft node failed, err %s", err.Error())
	}

	raftNode.addApplier(string(idgenerator.Module), idGenerator.GetSM())
	raftNode.addApplier(string(catalog.Module), newCatalog.GetSM())
	raftNode.addApplier(string(cluster.Module), newCluster.GetSM())

	groupCfg := &raft.GroupConfig{
		ID:      1,
		SM:      raftNode,
		Applied: raftNode.getApplyID(),
		Members: raftNode.getMembers(),
	}
	raftGroup := raftNode.createRaftGroup(ctx, groupCfg)

	idGenerator.SetRaftGroup(raftGroup)
	newCluster.SetRaftGroup(raftGroup)
	newCatalog.SetRaftGroup(raftGroup)

	raftNode.start(ctx)
	newCatalog.StartTask(ctx)

	return &Master{
		Catalog: newCatalog,
		Cluster: newCluster,
	}
}

func initConfig(cfg *Config) {
	cfg.StoreConfig.RaftOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.ColumnFamily = append(cfg.StoreConfig.RaftOption.ColumnFamily, raftWalCF)
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, catalog.CF, cluster.CF, idgenerator.CF, localCF)
}
