package catalog

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/client"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
)

type space struct {
	id             uint64
	inoLimit       uint64
	currentShardID uint32
	threshold      float64
	used           uint64

	info      *spaceInfo
	shards    *concurrentShards
	cluster   cluster.Cluster
	routerMgr Router

	store *store.Store

	serverClient client.Client

	state SpaceState

	lock sync.RWMutex
}

type SpaceConfig struct {
	ID          uint64
	Name        string
	InoLimit    uint64
	Desire      int
	SliceMapNum uint32
	SpaceType   proto.SpaceType
	Meta        []*FieldMeta
}

func NewSpace(ctx context.Context, cfg *SpaceConfig) (*space, error) {
	s := &space{
		id: cfg.ID,
		info: &spaceInfo{
			Sid:         cfg.ID,
			Name:        cfg.Name,
			FixedFields: cfg.Meta,
			Type:        cfg.SpaceType,
		},
		shards: newConcurrentShards(cfg.SliceMapNum),
	}
	return s, nil
}

func (s *space) AddShard(ctx context.Context) error {
	id := s.generateShardID()
	// todo 1. raft

	err := s.addShard(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

func (s *space) GetMeta(ctx context.Context) (*proto.SpaceMeta, error) {
	return nil, nil
}

func (s *space) Load(ctx context.Context) error {
	return nil
}

func (s *space) Report(ctx context.Context, reports []*ShardReport) error {
}

func (s *space) generateShardID() uint32 {
	return atomic.AddUint32(&s.currentShardID, 1)
}

// concurrentShards is an effective data struct (concurrent map implements)
type concurrentShards struct {
	total uint32
	num   uint32
	m     map[uint32]map[uint32]*shard
	locks map[uint32]*sync.RWMutex
}

func newConcurrentShards(splitMapNum uint32) *concurrentShards {
	m := &concurrentShards{
		num:   splitMapNum,
		m:     make(map[uint32]map[uint32]*shard),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		m.locks[i] = &sync.RWMutex{}
		m.m[i] = make(map[uint32]*shard)
	}
	return m
}

// Get shard from concurrentShards
func (s *concurrentShards) Get(sid uint32) *shard {
	idx := sid % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][sid]
}

// Put new shard into concurrentShards
func (s *concurrentShards) Put(v *shard) {
	id := v.id
	idx := id % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][id]
	// shard already exist
	if ok {
		return
	}
	atomic.AddUint32(&s.total, 1)
	s.m[idx][id] = v
}

// Range concurrentShards, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentShards) Range(f func(v *shard) error) {
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

func (s *concurrentShards) List() []*shard {
	res := make([]*shard, 0, atomic.LoadUint32(&s.total))
	s.Range(func(v *shard) error {
		res = append(res, v)
		return nil
	})
	return res
}
