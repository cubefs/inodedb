package catalog

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"golang.org/x/sync/singleflight"

	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type Config struct {
	MasterConfig sc.MasterConfig   `json:"master_config"`
	NodeConfig   proto.Node        `json:"node_config"`
	ServerConfig ShardServerConfig `json:"server_config"`
}

type Catalog struct {
	routeVersion uint64

	transporter  *transport
	masterClient *sc.MasterClient
	spaces       *concurrentSpaces
	singleRun    *singleflight.Group
}

func NewCatalog(ctx context.Context, cfg *Config) *Catalog {
	span := trace.SpanFromContext(ctx)

	masterClient, err := sc.NewMasterClient(&cfg.MasterConfig)
	if err != nil {
		span.Fatalf("new master transport failed: %s", err)
	}

	cfg.ServerConfig.MasterClient = masterClient
	if cfg.NodeConfig.GrpcPort == 0 {
		span.Fatalf("invalid node[%+v] config port", cfg.NodeConfig)
	}

	tr, err := NewTransport(&cfg.ServerConfig, &cfg.NodeConfig)
	if err != nil {
		span.Fatalf("new transport failed: %s", err)
	}
	if err = tr.Register(ctx); err != nil {
		span.Fatalf("register router to master failed: %s", err)
	}

	catalog := &Catalog{
		transporter:  tr,
		masterClient: masterClient,
		singleRun:    &singleflight.Group{},
		spaces:       newConcurrentSpaces(defaultSplitMapNum),
	}

	// heartbeat to master
	catalog.transporter.StartHeartbeat(ctx)

	// start get all change of catalog
	err = catalog.GetCatalogChanges(ctx)
	if err != nil {
		span.Fatalf("get catalog change failed: %s", err)
	}

	return catalog
}

func (c *Catalog) GetSpace(ctx context.Context, spaceName string) (*Space, error) {
	span := trace.SpanFromContextSafe(ctx)
	space := c.spaces.GetByName(spaceName)
	if space != nil {
		return space, nil
	}
	err := c.GetCatalogChanges(ctx)
	if err != nil {
		span.Errorf("get catalog change from master failed, err: %v", err)
		return nil, errors.ErrSpaceDoesNotExist
	}
	space = c.spaces.GetByName(spaceName)
	if space != nil {
		return space, nil
	}

	return nil, errors.ErrSpaceDoesNotExist
}

func (c *Catalog) GetCatalogChanges(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	routeVersion := atomic.LoadUint64(&c.routeVersion)
	_, err, _ := c.singleRun.Do(strconv.Itoa(int(routeVersion)), func() (interface{}, error) {
		catalogChanges, err := c.masterClient.GetCatalogChanges(ctx, &proto.GetCatalogChangesRequest{
			RouteVersion: routeVersion,
		})
		if err != nil {
			span.Errorf("get catalog change from master failed, version: %d, err: %v", routeVersion, err)
			return nil, err
		}

		if catalogChanges.RouteVersion <= routeVersion {
			return nil, nil
		}

		for _, item := range catalogChanges.Items {
			if item.RouteVersion < routeVersion {
				continue
			}
			switch item.Type {
			case proto.CatalogChangeType_AddSpace:
				spaceItem := new(proto.CatalogChangeSpaceAdd)
				if err = item.Item.UnmarshalTo(spaceItem); err != nil {
					return nil, err
				}
				space := newSpace(ctx, spaceItem.Name, spaceItem.Sid, c.GetCatalogChanges)
				c.spaces.Put(space)

			case proto.CatalogChangeType_DeleteSpace:
				spaceItem := new(proto.CatalogChangeSpaceDelete)
				if err = item.Item.UnmarshalTo(spaceItem); err != nil {
					return nil, err
				}
				c.spaces.Delete(spaceItem.Sid)

			case proto.CatalogChangeType_AddShard:
				shardItem := new(proto.CatalogChangeShardAdd)
				if err = item.Item.UnmarshalTo(shardItem); err != nil {
					return nil, err
				}
				space := c.spaces.Get(shardItem.Sid)
				space.AddShard(&ShardInfo{
					ShardId:  shardItem.ShardId,
					Epoch:    shardItem.Epoch,
					Nodes:    shardItem.Nodes,
					LeaderId: shardItem.Leader,
				}, c.transporter)
			default:
				span.Panicf("parse catalog change from master failed, err: %s", errors.ErrUnknownOperationType)
			}
		}
		atomic.StoreUint64(&c.routeVersion, catalogChanges.RouteVersion)
		return nil, nil
	})

	return err
}

func (c *Catalog) Close() {
	c.transporter.Close()
}

// concurrentSpaces is an effective data struct (concurrent map implements)
type concurrentSpaces struct {
	num     uint32
	idMap   map[uint32]map[uint64]*Space
	nameMap map[uint32]map[string]*Space
	locks   map[uint32]*sync.RWMutex
}

func newConcurrentSpaces(splitMapNum uint32) *concurrentSpaces {
	spaces := &concurrentSpaces{
		num:     splitMapNum,
		idMap:   make(map[uint32]map[uint64]*Space),
		nameMap: make(map[uint32]map[string]*Space),
		locks:   make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		spaces.locks[i] = &sync.RWMutex{}
		spaces.idMap[i] = make(map[uint64]*Space)
		spaces.nameMap[i] = make(map[string]*Space)
	}
	return spaces
}

// Get space from concurrentSpaces
func (s *concurrentSpaces) Get(sid uint64) *Space {
	idx := uint32(sid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.idMap[idx][sid]
}

func (s *concurrentSpaces) GetNoLock(sid uint64) *Space {
	idx := uint32(sid) % s.num
	return s.idMap[idx][sid]
}

func (s *concurrentSpaces) GetByName(name string) *Space {
	idx := s.nameCharSum(name) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.nameMap[idx][name]
}

func (s *concurrentSpaces) GetByNameNoLock(name string) *Space {
	idx := s.nameCharSum(name) % s.num
	return s.nameMap[idx][name]
}

// Put new space into concurrentSpaces
func (s *concurrentSpaces) Put(v *Space) {
	id := v.sid
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	s.idMap[idx][id] = v
	s.locks[idx].Unlock()

	idx = s.nameCharSum(v.name) % s.num
	s.locks[idx].Lock()
	s.nameMap[idx][v.name] = v
	s.locks[idx].Unlock()
}

// Delete space into concurrentSpaces
func (s *concurrentSpaces) Delete(id uint64) {
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	v := s.idMap[idx][id]
	delete(s.idMap[idx], id)
	s.locks[idx].Unlock()

	idx = s.nameCharSum(v.name) % s.num
	s.locks[idx].Lock()
	s.nameMap[idx][v.name] = v
	s.locks[idx].Unlock()
}

// Range concurrentSpaces, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentSpaces) Range(f func(v *Space) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.idMap[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}

func (s *concurrentSpaces) nameCharSum(name string) (ret uint32) {
	for i := range name {
		ret += uint32(name[i])
	}
	return
}
