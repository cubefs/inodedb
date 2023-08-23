package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/proto"
)

type CreateSpaceArgs struct {
	SpaceName     string
	SpaceType     proto.SpaceType
	DesiredShards uint32
	FixedFields   []proto.FieldMeta
}

type catalog struct {
	spaces *concurrentSpaces

	done      chan struct{}
	raftGroup raft.Group

	lock sync.RWMutex
}

type Catalog interface {
	CreateSpace(ctx context.Context, args *CreateSpaceArgs) error
	DeleteSpace(ctx context.Context, sid uint64) error
	GetSpace(ctx context.Context, sid uint64) (*proto.SpaceMeta, error)
	Report(ctx context.Context, nodeId uint32, infos []*proto.ShardReport) error
	GetCatalogChanges(ctx context.Context, routerVersion uint64, nodeId uint32) ([]*proto.CatalogChangeItem, error)
	Close()
}

type Config struct {
	CurrentSpaceID uint64 `json:"current_space_id"`
	Name           string `json:"name"`
}

func NewCatalog(ctx context.Context, cfg *Config) Catalog {
	return nil
}

func (c *catalog) CreateSpace(ctx context.Context, args *CreateSpaceArgs) error {
	return nil
}

func (c *catalog) DeleteSpace(ctx context.Context, spaceID uint64) error {
	return nil
}

func (c *catalog) GetSpace(ctx context.Context, spaceID uint64) (*proto.SpaceMeta, error) {
	return nil, nil
}

func (c *catalog) Report(ctx context.Context, nodeId uint32, infos []*proto.ShardReport) error {
	return nil
}

func (c *catalog) GetCatalogChanges(ctx context.Context, routerVersion uint64, nodeId uint32) ([]*proto.CatalogChangeItem, error) {
	return nil, nil
}

func (c *catalog) Close() {
	close(c.done)
}

func (c *catalog) generateSpaceID() uint64 {
	return 0
}

// concurrentSpaces is an effective data struct (concurrent map implements)
type concurrentSpaces struct {
	num   uint32
	m     map[uint32]map[uint64]*space
	locks map[uint32]*sync.RWMutex
}

func newConcurrentSpaces(splitMapNum uint32) *concurrentSpaces {
	spaces := &concurrentSpaces{
		num:   splitMapNum,
		m:     make(map[uint32]map[uint64]*space),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		spaces.locks[i] = &sync.RWMutex{}
		spaces.m[i] = make(map[uint64]*space)
	}
	return spaces
}

// Get space from concurrentSpaces
func (s *concurrentSpaces) Get(sid uint64) *space {
	idx := uint32(sid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][sid]
}

// Put new space into shardedSpace
func (s *concurrentSpaces) Put(v *space) {
	id := v.id
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][id]
	// space already exist
	if ok {
		return
	}
	s.m[idx][id] = v
}

// Delete space into shardedSpace
func (s *concurrentSpaces) Delete(id uint64) {
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	delete(s.m[idx], id)
}

// Range concurrentSpaces, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentSpaces) Range(f func(v *space) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.m[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}
