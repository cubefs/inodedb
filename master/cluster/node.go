package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/inodedb/proto"
)

type node struct {
	info   *nodeInfo
	nodeId uint32

	shardCount int32
	expires    time.Time
	lock       sync.RWMutex
}

func (n *node) HandleHeartbeat(ctx context.Context, shardCount int32, time time.Time) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.expires = time

	if n.info.State != proto.NodeState_Alive {
		n.info.State = proto.NodeState_Alive
	}
	atomic.StoreInt32(&n.shardCount, shardCount)
}

func (n *node) IsAvailable() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	if n.isExpire() {
		return false
	}
	return n.info.State == proto.NodeState_Alive
}

func (n *node) Contains(ctx context.Context, role proto.NodeRole) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, r := range n.info.Roles {
		if r == role {
			return true
		}
	}
	return false
}

func (n *node) UpdateShardCount(delta int32) {
	atomic.AddInt32(&n.shardCount, delta)
}

func (n *node) GetInfo() *nodeInfo {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.info.Clone()
}

func (n *node) GetShardCount() int32 {
	return atomic.LoadInt32(&n.shardCount)
}

func (n *node) isExpire() bool {
	if n.expires.IsZero() {
		return false
	}
	return time.Since(n.expires) > 0
}

// concurrentNodes is an effective data struct (concurrent map implements)
type concurrentNodes struct {
	idMap   map[uint32]*node
	addrMap map[string]*node
	lock    sync.RWMutex
}

func newConcurrentNodes(splitMapNum uint32) *concurrentNodes {
	spaces := &concurrentNodes{
		idMap:   make(map[uint32]*node),
		addrMap: make(map[string]*node),
	}
	return spaces
}

// Get space from concurrentNodes
func (s *concurrentNodes) Get(nodeId uint32) *node {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.idMap[nodeId]
}

func (s *concurrentNodes) GetNoLock(nodeId uint32) *node {
	return s.idMap[nodeId]
}

func (s *concurrentNodes) GetByName(addr string) *node {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.addrMap[addr]
}

func (s *concurrentNodes) GetByNameNoLock(addr string) *node {
	return s.addrMap[addr]
}

// PutNoLock new space into shardedSpace with no lock
func (s *concurrentNodes) PutNoLock(v *node) {
	id := v.nodeId
	s.idMap[id] = v
	s.addrMap[v.info.Addr] = v
}

// DeleteNoLock node in concurrentNodes with no lock
func (s *concurrentNodes) DeleteNoLock(id uint32) {
	v := s.idMap[id]
	delete(s.idMap, id)
	delete(s.addrMap, v.info.Addr)
}

// Range concurrentNodes, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentNodes) Range(f func(v *node) error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, v := range s.idMap {
		err := f(v)
		if err != nil {
			return
		}
	}
}

func (s *concurrentNodes) Lock() {
	s.lock.Lock()
}

func (s *concurrentNodes) Unlock() {
	s.lock.Unlock()
}
