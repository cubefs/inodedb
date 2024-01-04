package catalog

import (
	"sync"

	"github.com/cubefs/inodedb/proto"
)

type shard struct {
	id     uint32
	leader uint32
	info   *shardInfo
	lock   sync.RWMutex
}

func newShard(shardInfo *shardInfo) *shard {
	return &shard{
		id:     shardInfo.ShardID,
		leader: shardInfo.Leader,
		info:   shardInfo,
	}
}

func (s *shard) GetInfo() *shardInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return &(*s.info)
}

func (s *shard) UpdateReportInfoNoLock(info proto.Shard) {
	s.info.InoUsed = info.InoUsed
	s.info.Leader = info.LeaderID
}
