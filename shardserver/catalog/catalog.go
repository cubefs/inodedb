package catalog

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/client"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/store"
)

const defaultBTreeDegree = 32

type (
	Config struct {
		StoreConfig  store.Config        `json:"store_config"`
		MasterConfig client.MasterConfig `json:"master_config"`
		NodeConfig   proto.Node          `json:"node_config"`
	}
)

type Catalog struct {
	routeVersion uint64
	transporter  *transporter
	store        *store.Store

	spaces         sync.Map
	spaceIdToNames sync.Map
	done           chan struct{}
}

func NewCatalog(ctx context.Context, cfg *Config) *Catalog {
	span := trace.SpanFromContext(ctx)
	masterClient, err := client.NewMasterClient(&cfg.MasterConfig)
	if err != nil {
		span.Fatalf("new master client failed: %s", err)
	}

	if cfg.NodeConfig.GrpcPort == 0 || cfg.NodeConfig.RaftPort == 0 {
		span.Fatalf("invalid node[%+v] config port", cfg.NodeConfig)
	}

	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, lockCF, dataCF, writeCF)
	store, err := store.NewStore(ctx, &cfg.StoreConfig)
	if err != nil {
		span.Fatalf("new store instance failed: %s", err)
	}

	transporter := newTransporter(masterClient, &cfg.NodeConfig)
	if err := transporter.Register(ctx); err != nil {
		span.Fatalf("register shard server failed: %s", err)
	}

	catalog := &Catalog{
		transporter: transporter,
		store:       store,
		done:        make(chan struct{}),
	}
	if err := catalog.initRoute(ctx); err != nil {
		span.Fatalf("update route failed: %s", errors.Detail(err))
	}

	catalog.transporter.StartHeartbeat(ctx)
	go catalog.loop(ctx)
	return catalog
}

func (c *Catalog) GetSpace(ctx context.Context, spaceName string) (*Space, error) {
	return c.getSpace(ctx, spaceName)
}

func (c *Catalog) AddShard(ctx context.Context, spaceName string, shardId uint32, epoch uint64, inoLimit uint64, replicates []uint32) error {
	span := trace.SpanFromContext(ctx)
	v, ok := c.spaces.Load(spaceName)
	if ok {
		space := v.(*Space)
		return space.AddShard(ctx, shardId, epoch, inoLimit, replicates)
	}

	if err := c.updateSpace(ctx, spaceName); err != nil {
		span.Warnf("update route failed: %s", err)
		return apierrors.ErrSpaceDoesNotExist
	}

	v, ok = c.spaces.Load(spaceName)
	if !ok {
		span.Warnf("still can not get route update for space[%s]", spaceName)
		return apierrors.ErrSpaceDoesNotExist
	}
	space := v.(*Space)
	return space.AddShard(ctx, shardId, epoch, inoLimit, replicates)
}

func (c *Catalog) UpdateShard(ctx context.Context, spaceName string, shardId uint32, epoch uint64) error {
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		return apierrors.ErrSpaceNotExist
	}

	space := v.(*Space)
	return space.UpdateShard(ctx, shardId, epoch)
}

func (c *Catalog) GetShard(ctx context.Context, spaceName string, shardID uint32) (*proto.Shard, error) {
	shard, err := c.getShard(ctx, spaceName, shardID)
	if err != nil {
		return nil, err
	}

	shardStat := shard.Stats()
	// transform into external nodes
	nodes := make([]*proto.Node, 0, len(shardStat.nodes))
	for _, nodeId := range shardStat.nodes {
		nodeInfo, err := c.transporter.GetNode(ctx, nodeId)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nodeInfo)
	}

	return &proto.Shard{
		Id:       shard.ShardId,
		InoLimit: shardStat.inoLimit,
		InoUsed:  shardStat.inoUsed,
		Nodes:    nodes,
	}, nil
}

func (c *Catalog) loop(ctx context.Context) {
	reportTicker := time.NewTicker(60 * time.Second)
	routeUpdateTicker := time.NewTicker(5 * time.Second)
	defer func() {
		reportTicker.Stop()
		routeUpdateTicker.Stop()
	}()

	for {
		select {
		case <-reportTicker.C:
			span, ctx := trace.StartSpanFromContext(ctx, "")
			shardReports := c.getAlteredShards()
			tasks, err := c.transporter.Report(ctx, shardReports)
			if err != nil {
				span.Warnf("shard report failed: %s", err)
				continue
			}
			for _, task := range tasks {
				c.executeShardTask(ctx, task)
			}

			reportTicker.Reset(time.Duration(60+rand.Intn(10)) * time.Second)
		case <-routeUpdateTicker.C:
			span, ctx := trace.StartSpanFromContext(ctx, "")
			if err := c.initRoute(ctx); err != nil {
				span.Warnf("update route failed: %s", err)
			}
			routeUpdateTicker.Reset(time.Duration(5+rand.Intn(5)) * time.Second)
		case <-c.done:
			return
		}
	}
}

func (c *Catalog) getSpace(ctx context.Context, spaceName string) (*Space, error) {
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		return nil, apierrors.ErrSpaceDoesNotExist
	}

	space := v.(*Space)
	return space, nil
}

// TODO: get altered shards, optimized the load of master
func (c *Catalog) getAlteredShards() []*proto.ShardReport {
	ret := make([]*proto.ShardReport, 0, 1<<10)
	c.spaces.Range(func(key, value interface{}) bool {
		space := value.(*Space)
		space.shards.Range(func(key, value interface{}) bool {
			shard := value.(*shard)
			stats := shard.Stats()

			ret = append(ret, &proto.ShardReport{
				Sid: space.sid,
				Shard: &proto.Shard{
					Epoch:    stats.routeVersion,
					Id:       shard.ShardId,
					InoLimit: stats.inoLimit,
					InoUsed:  stats.inoUsed,
				},
			})

			return true
		})
		return true
	})
	return ret
}

func (c *Catalog) updateSpace(ctx context.Context, spaceName string) error {
	spaceMeta, err := c.transporter.GetSpace(ctx, spaceName)
	if err != nil {
		return err
	}

	fixedFields := make(map[string]proto.FieldMeta, len(spaceMeta.FixedFields))
	for _, field := range spaceMeta.FixedFields {
		fixedFields[field.Name] = *field
	}

	space := &Space{
		store:       c.store,
		sid:         spaceMeta.Sid,
		name:        spaceMeta.Name,
		spaceType:   spaceMeta.Type,
		fixedFields: fixedFields,
	}
	c.spaces.LoadOrStore(spaceMeta.Name, space)
	c.spaceIdToNames.LoadOrStore(spaceMeta.Sid, spaceMeta.Name)
	// load space's shards
	if err := space.Load(ctx); err != nil {
		return errors.Info(err, "load space failed")
	}

	return nil
}

func (c *Catalog) initRoute(ctx context.Context) error {
	routeVersion, changes, err := c.transporter.GetRouteUpdate(ctx, c.getRouteVersion())
	if err != nil {
		return errors.Info(err, "get route update failed")
	}

	for _, routeItem := range changes {
		switch routeItem.Type {
		case proto.CatalogChangeType_AddSpace:
			spaceItem := new(proto.CatalogChangeSpaceAdd)
			if err := routeItem.Item.UnmarshalTo(spaceItem); err != nil {
				return errors.Info(err, "unmarshal add space item failed")
			}

			fixedFields := make(map[string]proto.FieldMeta, len(spaceItem.FixedFields))
			for _, field := range spaceItem.FixedFields {
				fixedFields[field.Name] = *field
			}

			space := &Space{
				store:       c.store,
				sid:         spaceItem.Sid,
				name:        spaceItem.Name,
				spaceType:   spaceItem.Type,
				fixedFields: fixedFields,
			}
			c.spaces.LoadOrStore(spaceItem.Name, space)
			c.spaceIdToNames.LoadOrStore(spaceItem.Sid, spaceItem.Name)
			// load space's shards
			if err := space.Load(ctx); err != nil {
				return errors.Info(err, "load space failed")
			}

		case proto.CatalogChangeType_DeleteSpace:
			spaceItem := new(proto.CatalogChangeSpaceDelete)
			if err := routeItem.Item.UnmarshalTo(spaceItem); err != nil {
				return errors.Info(err, "unmarshal delete space item failed")
			}

			spaceName, _ := c.spaceIdToNames.Load(spaceItem.Sid)
			c.spaces.Delete(spaceName.(string))
			c.spaceIdToNames.Delete(spaceItem.Sid)
		default:
		}
		c.updateRouteVersion(routeItem.RouteVersion)
	}

	c.updateRouteVersion(routeVersion)
	return nil
}

func (c *Catalog) executeShardTask(ctx context.Context, task *proto.ShardTask) error {
	switch task.Type {
	case proto.ShardTaskType_ClearShard:
		space, err := c.GetSpace(ctx, task.SpaceName)
		if err != nil {
			return err
		}
		shard, err := space.GetShard(ctx, task.ShardId)
		if err != nil {
			return err
		}

		if shard.GetEpoch() == task.Epoch {
			shard.Stop()
			shard.Close()
			return space.DeleteShard(ctx, task.ShardId)
		}
	default:
	}
	return nil
}

func (c *Catalog) getRouteVersion() uint64 {
	return atomic.LoadUint64(&c.routeVersion)
}

func (c *Catalog) updateRouteVersion(new uint64) {
	old := atomic.LoadUint64(&c.routeVersion)
	if old < new {
		for {
			// update success, break
			if atomic.CompareAndSwapUint64(&c.routeVersion, old, new) {
				break
			}
			// already update, break
			old = atomic.LoadUint64(&c.routeVersion)
			if old >= new {
				break
			}
			// otherwise, retry cas
		}
	}
}

func (c *Catalog) getShard(ctx context.Context, spaceName string, shardId uint32) (*shard, error) {
	space, err := c.getSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}

	return space.GetShard(ctx, shardId)
}

func isReplicateMember(target uint32, replicates []uint32) bool {
	for _, replicate := range replicates {
		if replicate == target {
			return true
		}
	}
	return false
}
