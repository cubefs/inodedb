package catalog

import (
	"context"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/raft"
	"github.com/cubefs/inodedb/proto"
)

type catalog struct {
	info      *CatalogInfo
	allSpaces *shardedSpaces

	cache sync.Map

	raft      raft.Raft
	routerMgr Router

	closeChan      chan struct{}
	expChan        chan Space
	currentSpaceID uint64

	lock sync.RWMutex
}

type Catalog interface {
	CreateSpace(ctx context.Context, Name string) error
	DeleteSpace(ctx context.Context, spaceID uint64) error
	GetSpace(ctx context.Context, spaceID uint64) (*proto.SpaceMeta, error)
	Report(ctx context.Context, args *ReportArgs) error
	Close()
}

type Config struct {
	CurrentSpaceID uint64 `json:"current_space_id"`
	Name           string `json:"name"`
}

func NewCatalog(ctx context.Context, cfg *Config) Catalog {
	c := &catalog{
		info: &CatalogInfo{
			Name:       cfg.Name,
			CreateTime: time.Now().UnixMilli(),
		},
		currentSpaceID: cfg.CurrentSpaceID,
	}
	c.loop(ctx)
	c.loopAddShard()
	return c
}

func (c *catalog) CreateSpace(ctx context.Context, name string) error {
	spaceID := c.generateSpaceID()
	// todo 1. raft
	sp, err := NewSpace(ctx, &SpaceConfig{Name: name, ID: spaceID}, c.expChan)
	if err != nil {
		return err
	}
	// todo 2. persistent

	// todo 3. add router
	c.allSpaces.putSpace(ctx, sp)

	return nil
}

func (c *catalog) List(ctx context.Context) []*SpaceInfo {
	return nil
}

func (c *catalog) DeleteSpace(ctx context.Context, spaceID uint64) error {
	// todo 1. raft
	c.allSpaces.deleteSpace(spaceID)
	return nil
}

func (c *catalog) GetSpace(ctx context.Context, spaceID uint64) (*proto.SpaceMeta, error) {

	if store, loaded := c.cache.Load(spaceID); loaded {
		return store.(Space).GetMeta(ctx)
	}
	getSpace := c.allSpaces.getSpace(spaceID)
	if getSpace == nil {
		return nil, errors.ErrSpaceNotExist
	}
	c.cache.Store(spaceID, getSpace)
	return getSpace.GetMeta(ctx)
}

func (c *catalog) Report(ctx context.Context, args *ReportArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("receive report info from node[%d]", args.Id)
	sm := make(map[uint64][]*ShardReport)
	for _, info := range args.Infos {
		sm[info.SpaceID] = append(sm[info.SpaceID], info)
	}
	for id, shards := range sm {
		getSpace := c.allSpaces.getSpace(id)
		if getSpace == nil {
			return errors.ErrSpaceDoesNotExist
		}
		err := getSpace.Report(ctx, shards)
		if err != nil {
			span.Errorf("handle report from node[%d], space[%d] failed, err: %s", args.Id, id, err)
			continue
		}
	}
	return nil
}

func (c *catalog) generateSpaceID() uint64 {
	return atomic.AddUint64(&c.currentSpaceID, 1)
}

func (c *catalog) Close() {
	close(c.closeChan)
}

// background task
func (c *catalog) loop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				// todo background task

			case <-c.closeChan:
				return
			}
		}
	}()
}

func (c *catalog) loopAddShard() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "add shard")
	go func() {
		for {
			select {
			case sp := <-c.expChan:
				if err := sp.AddShard(ctx); err != nil {
					span.Errorf("add shard failed for space[%d]", sp.ID(ctx))
				}
			}
		}
	}()
}
