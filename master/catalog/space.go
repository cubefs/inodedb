package catalog

import (
	"sync"
	"sync/atomic"

	"github.com/cubefs/inodedb/proto"
)

type SpaceConfig struct {
	ID          uint64
	Name        string
	InoLimit    uint64
	Desire      int
	SliceMapNum uint32
	SpaceType   proto.SpaceType
	Meta        []*FieldMeta
}

type space struct {
	id              uint64
	info            *spaceInfo
	shards          *concurrentShards
	expandingShards []*shard
	currentShardId  uint32

	lock sync.RWMutex
}

func newSpace(spaceInfo *spaceInfo) *space {
	space := &space{
		id:     spaceInfo.Sid,
		info:   spaceInfo,
		shards: newConcurrentShards(defaultSplitMapNum),
	}
	return space
}

func (s *space) GetInfo() *spaceInfo {
	s.lock.RLock()
	s.lock.RUnlock()

	return &(*s.info)
}

func (s *space) GetShard(shardId uint32) *shard {
	return s.shards.Get(shardId)
}

func (s *space) GetCurrentShardId() uint32 {
	s.lock.RLock()
	id := s.currentShardId
	s.lock.RUnlock()

	return id
}

func (s *space) PutShard(shard *shard) {
	s.shards.Put(shard)

	s.lock.Lock()
	if shard.id > s.currentShardId {
		s.currentShardId = shard.id
	}
	s.lock.Unlock()
}

func (s *space) GetAllShards() []*shard {
	return s.shards.List()
}

func (s *space) GetExpandingShards() []*shard {
	return s.expandingShards
}

func (s *space) IsNormal() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.info.Status == SpaceStatusNormal
}

func (s *space) IsInit(withLock bool) bool {
	if withLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.info.Status == SpaceStatusInit
}

func (s *space) IsUpdateRoute(withLock bool) bool {
	if withLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.info.Status == SpaceStatusUpdateRoute
}

func (s *space) IsExpandUpdateNone(withLock bool) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.info.ExpandStatus == SpaceExpandStatusNone
}

func (s *space) IsExpandUpdateRoute(withLock bool) bool {
	if withLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.info.ExpandStatus == SpaceExpandStatusUpdateRoute
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
