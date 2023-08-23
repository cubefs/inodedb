package catalog

import (
	"sync"
)

type shard struct {
	id     uint32
	leader uint32
	info   *shardInfo
	lock   sync.RWMutex
}

// Update Statistics to incrementally calculate the water level of the current space
func (s *shard) Update(info *shardInfo, leader uint32) int64 {
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
