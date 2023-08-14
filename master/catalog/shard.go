package catalog

import (
	"sync"
	"sync/atomic"
)

type Shard interface {
	ID() uint32
	Info() *ShardInfo
	Leader() (uint32, error)
	Update(info *ShardInfo, leader uint32) int64
}

type shard struct {
	shardID uint32
	leader  uint32
	info    *ShardInfo
	lock    sync.RWMutex
}

func (s *shard) ID() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.shardID
}

func (s *shard) Info() *ShardInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.info
}

func (s *shard) Leader() (uint32, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.leader, nil
}

// Update Statistics to incrementally calculate the water level of the current space
func (s *shard) Update(info *ShardInfo, leader uint32) int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	var sub int64

	s.leader = leader
	if info.InoUsed >= s.info.InoUsed {
		sub = int64(info.InoUsed - s.info.InoUsed)
		s.info = info
		return sub
	}
	sub = -int64(s.info.InoUsed - info.InoUsed)
	s.info = info
	return sub
}

// shardedShards is an effective data struct (concurrent map implements)
type shardedShards struct {
	total uint32
	num   uint32
	m     map[uint32]map[uint32]Shard
	locks map[uint32]*sync.RWMutex
}

func newShardedShards(sliceMapNum uint32) *shardedShards {
	m := &shardedShards{
		num:   sliceMapNum,
		m:     make(map[uint32]map[uint32]Shard),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < sliceMapNum; i++ {
		m.locks[i] = &sync.RWMutex{}
		m.m[i] = make(map[uint32]Shard)
	}
	return m
}

// Get shard from shardedShards
func (s *shardedShards) Get(sid uint32) Shard {
	idx := sid % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][sid]
}

// Put new shard into shardedShards
func (s *shardedShards) Put(v Shard) {
	id := v.ID()
	idx := v.ID() % s.num
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

// Range shardedShards, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *shardedShards) Range(f func(v Shard) error) {
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

func (s *shardedShards) List() []*ShardInfo {
	res := make([]*ShardInfo, 0, atomic.LoadUint32(&s.total))
	s.Range(func(v Shard) error {
		res = append(res, v.Info())
		return nil
	})
	return res
}
