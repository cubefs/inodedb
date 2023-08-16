package catalog

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shard/store"
)

type Config struct {
	StorePath  string   `json:"store_path"`
	MasterAddr []string `json:"master_addr"`
}

type Catalog struct {
	routeVersion uint64
	transport    *transport
	store        *store.Store

	spaces sync.Map
	done   chan struct{}
}

func NewCatalog(cfg Config) *Catalog {
	return &Catalog{}
}

func (c *Catalog) GetSpace(ctx context.Context, spaceName string) (*Space, error) {
	return c.getSpace(ctx, spaceName)
}

func (c *Catalog) AddShard(ctx context.Context, spaceName string, shardId uint32, routeVersion uint64, inoLimit uint64, replicates map[uint32]string) error {
	span := trace.SpanFromContext(ctx)
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		if err := c.updateRoute(ctx); err != nil {
			span.Warnf("update route failed: %s", err)
			return errors.ErrSpaceDoesNotExist
		}

		v, ok = c.spaces.Load(spaceName)
		if !ok {
			span.Warnf("still can not get route update for Space[%s]", spaceName)
			return errors.ErrSpaceDoesNotExist
		}
	}

	space := v.(*Space)
	space.AddShard(ctx, shardId, routeVersion, inoLimit, replicates)
	return nil
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
		nodeInfo, err := c.transport.GetNode(ctx, nodeId)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, internalNodeInfoToExternalNode(nodeInfo))
	}

	return &proto.Shard{
		Id:       shard.shardId,
		InoLimit: shardStat.inoLimit,
		InoUsed:  shardStat.inoUsed,
		Nodes:    nodes,
	}, nil
}

func (c *Catalog) loop(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	reportTicker := time.NewTicker(60 * time.Second)
	routeUpdateTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-reportTicker.C:
			shardReports := c.getAlteredShards()
			if err := c.transport.Report(ctx, shardReports); err != nil {
				span.Warnf("shard report failed: %s", err)
			}
			reportTicker.Reset(time.Duration(60+rand.Intn(10)) * time.Second)
		case <-routeUpdateTicker.C:
			if err := c.updateRoute(ctx); err != nil {
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
		return nil, errors.ErrSpaceDoesNotExist
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

			ret = append(ret, &proto.ShardReport{Shard: &proto.Shard{
				RouteVersion: stats.routeVersion,
				Id:           shard.shardId,
				InoLimit:     stats.inoLimit,
				InoUsed:      stats.inoUsed,
			}})

			return true
		})
		return true
	})
	return ret
}

func (c *Catalog) updateRoute(ctx context.Context) error {
	changes, err := c.transport.GetRouteUpdate(ctx, atomic.LoadUint64(&c.routeVersion))
	if err != nil {
		return err
	}

	for _, routeItem := range changes {
		if routeItem.RouteVersion <= atomic.LoadUint64(&c.routeVersion) {
			continue
		}
		switch routeItem.Type {
		case proto.CatalogChangeType_AddSpace:
			spaceItem := new(proto.CatalogChangeSpaceAdd)
			if err := routeItem.Item.UnmarshalTo(spaceItem); err != nil {
				return err
			}

			fixedFields := make(map[string]proto.FieldMeta, len(spaceItem.FixedFields))
			for _, field := range spaceItem.FixedFields {
				fixedFields[field.Name] = *field
			}

			c.spaces.LoadOrStore(spaceItem.Name, &Space{
				store:       c.store,
				sid:         spaceItem.Sid,
				name:        spaceItem.Name,
				spaceType:   spaceItem.Type,
				fixedFields: fixedFields,
			})
		case proto.CatalogChangeType_DeleteSpace:
			spaceItem := new(proto.CatalogChangeSpaceDelete)
			if err := routeItem.Item.UnmarshalTo(spaceItem); err != nil {
				return err
			}

			c.spaces.Delete(spaceItem.Name)
		}
		c.updateRouteVersion(routeItem.RouteVersion)
	}
	return nil
}

func (c *Catalog) updateRouteVersion(new uint64) {
	old := atomic.LoadUint64(&c.routeVersion)
	if old < new {
		for {
			// update success, break
			if isSwap := atomic.CompareAndSwapUint64(&c.routeVersion, old, new); isSwap {
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

func internalNodeInfoToExternalNode(info *nodeInfo) *proto.Node {
	return &proto.Node{
		Id:       info.id,
		Addr:     info.addr,
		GrpcPort: info.grpcPort,
		HttpPort: info.httpPort,
		RaftPort: info.raftPort,
		Az:       info.az,
		Role:     info.role,
		State:    info.state,
	}
}
