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
	"github.com/cubefs/inodedb/server/store"
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

func (c *Catalog) AddShard(ctx context.Context, spaceName string, shardId uint32, inoLimit uint64, replicates map[uint32]string) error {
	span := trace.SpanFromContext(ctx)
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		if err := c.updateRoute(ctx); err != nil {
			span.Warnf("update route failed: %s", err)
			return errors.ErrSpaceDoesNotExist
		}
		v, ok = c.spaces.Load(spaceName)
		if !ok {
			span.Warnf("still can not get route update for space[%s]", spaceName)
			return errors.ErrSpaceDoesNotExist
		}
	}
	space := v.(*space)
	space.AddShard(ctx, shardId, inoLimit, replicates)
	return nil
}

func (c *Catalog) GetShard(ctx context.Context, spaceName string, shardID uint32) (*proto.Shard, error) {
	shard, err := c.getShard(ctx, spaceName, shardID)
	if err != nil {
		return nil, err
	}
	shardStat := shard.Stats()
	// TODO: transform replicates into nodes

	return &proto.Shard{
		ShardId:    shard.shardId,
		InoLimit:   shardStat.inoLimit,
		InoUsed:    shardStat.inoUsed,
		Replicates: nil,
	}, nil
}

func (c *Catalog) InsertItem(ctx context.Context, spaceName string, shardId uint32, item *proto.Item) (uint64, error) {
	space, err := c.getSpace(ctx, spaceName)
	if err != nil {
		return 0, err
	}
	if !space.ValidateFields(item.Fields) {
		return 0, errors.ErrUnknownField
	}
	shard, err := space.GetShard(ctx, shardId)
	if err != nil {
		return 0, err
	}

	ino, err := shard.InsertItem(ctx, item)
	if err != nil {
		return 0, err
	}
	return ino, nil
}

func (c *Catalog) UpdateItem(ctx context.Context, spaceName string, item *proto.Item) error {
	space, err := c.getSpace(ctx, spaceName)
	if err != nil {
		return err
	}
	if !space.ValidateFields(item.Fields) {
		return errors.ErrUnknownField
	}
	shard := space.LocateShard(ctx, item.Ino)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	return shard.UpdateItem(ctx, item)
}

func (c *Catalog) DeleteItem(ctx context.Context, spaceName string, ino uint64) error {
	shard, err := c.locateShard(ctx, spaceName, ino)
	if err != nil {
		return err
	}
	return shard.DeleteItem(ctx, ino)
}

func (c *Catalog) GetItem(ctx context.Context, spaceName string, ino uint64) (*proto.Item, error) {
	shard, err := c.locateShard(ctx, spaceName, ino)
	if err != nil {
		return nil, err
	}
	return shard.GetItem(ctx, ino)
}

func (c *Catalog) Link(ctx context.Context, spaceName string, link *proto.Link) error {
	shard, err := c.locateShard(ctx, spaceName, link.Parent)
	if err != nil {
		return err
	}
	return shard.Link(ctx, link)
}

func (c *Catalog) Unlink(ctx context.Context, spaceName string, unlink *proto.Unlink) error {
	shard, err := c.locateShard(ctx, spaceName, unlink.Parent)
	if err != nil {
		return err
	}
	return shard.Unlink(ctx, unlink.Parent, unlink.Name)
}

func (c *Catalog) List(ctx context.Context, req *proto.ListRequest) ([]*proto.Link, error) {
	shard, err := c.locateShard(ctx, req.SpaceName, req.Ino)
	if err != nil {
		return nil, err
	}
	return shard.List(ctx, req.Ino, req.Start, req.Num)
}

func (c *Catalog) Search(ctx context.Context) error {
	// todo
	return nil
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

func (c *Catalog) getSpace(ctx context.Context, spaceName string) (*space, error) {
	v, ok := c.spaces.Load(spaceName)
	if !ok {
		return nil, errors.ErrSpaceDoesNotExist
	}
	space := v.(*space)
	return space, nil
}

// TODO: get altered shards, optimized the load of master
func (c *Catalog) getAlteredShards() []*proto.ShardReport {
	return nil
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
		case proto.RouteItemType_AddSpace:
			spaceItem := new(proto.RouteItemSpaceAdd)
			if err := routeItem.Item.UnmarshalTo(spaceItem); err != nil {
				return err
			}
			fixedFields := make(map[string]proto.FieldMeta, len(spaceItem.FixedFields))
			for _, field := range spaceItem.FixedFields {
				fixedFields[field.Name] = *field
			}
			c.spaces.LoadOrStore(spaceItem.Name, &space{
				store:       c.store,
				sid:         spaceItem.Sid,
				name:        spaceItem.Name,
				spaceType:   spaceItem.Type,
				fixedFields: fixedFields,
			})
		case proto.RouteItemType_DeleteSpace:
			spaceItem := new(proto.RouteItemSpaceDelete)
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
	shard, err := space.GetShard(ctx, shardId)
	if err != nil {
		return nil, err
	}
	return shard, nil
}

func (c *Catalog) locateShard(ctx context.Context, spaceName string, ino uint64) (*shard, error) {
	space, err := c.getSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	shard := space.LocateShard(ctx, ino)
	if shard == nil {
		return nil, errors.ErrInoRangeNotFound
	}
	return shard, nil
}
