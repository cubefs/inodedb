package route

import (
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/server/scalar"
	"github.com/cubefs/inodedb/server/store"
	"github.com/cubefs/inodedb/server/vector"
)

type shardMeta struct {
	id       uint32
	inoRange *proto.InoRange
}

func (s *shardMeta) Less(than btree.Item) bool {
	thanShard := than.(*shardMeta)
	return s.inoRange.StartIno < thanShard.inoRange.StartIno
}

func (s *shardMeta) Copy() btree.Item {
	return &(*s)
}

func newShard(shardID uint32, inoRange *proto.InoRange, replicates map[uint32]string) *shard {
	shard := &shard{
		shardMeta: &shardMeta{
			id:       shardID,
			inoRange: inoRange,
		},
		replicates: replicates,
	}
	return shard
}

type shardStat struct {
	inoUsed    uint64
	inoCursor  uint64
	inoRange   proto.InoRange
	replicates []uint32
}

type shard struct {
	*shardMeta

	inoCursor   uint64
	inoUsed     uint64
	replicates  map[uint32]string
	vectorIndex *vector.Index
	scalarIndex *scalar.Index
	store       *store.Store
	raftGroup   *RaftGroup

	mux sync.RWMutex
}

func (s *shard) Stat() *shardStat {
	s.mux.RLock()
	replicates := make([]uint32, 0, len(s.replicates))
	for i := range s.replicates {
		replicates = append(replicates, i)
	}
	inoRange := *s.inoRange
	s.mux.RUnlock()

	return &shardStat{
		inoUsed:    atomic.LoadUint64(&s.inoUsed),
		inoCursor:  atomic.LoadUint64(&s.inoCursor),
		inoRange:   inoRange,
		replicates: replicates,
	}
}

func (s *shard) Start() {
	// TODO: start raft group
}

func (s *shard) nextIno() (uint64, error) {
	if s.inoUsed > s.inoRange.InoLimit {
		return 0, errors.ErrInodeLimitExceed
	}
	for {
		cur := atomic.LoadUint64(&s.inoCursor)
		if cur >= s.inoRange.EndIno {
			return 0, errors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.inoCursor, cur, newId) {
			atomic.AddUint64(&s.inoUsed, 1)
			return newId, nil
		}
	}
}
