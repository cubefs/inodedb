package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/inodedb/proto"
)

type node struct {
	info   *NodeInfo
	nodeId uint32

	shardCount       int32
	heartbeatTimeout int
	expires          time.Time
	lock             sync.RWMutex
}

func (n *node) handleHeartbeat(ctx context.Context, shardCount int32) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.expires = time.Now().Add(time.Duration(n.heartbeatTimeout) * time.Second)

	if n.info.State != proto.NodeState_Alive {
		n.info.State = proto.NodeState_Alive
	}
	atomic.StoreInt32(&n.shardCount, shardCount)
}

func (n *node) isExpire() bool {
	if n.expires.IsZero() {
		return false
	}
	return time.Since(n.expires) > 0
}

func (n *node) isAvailable() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	if n.isExpire() {
		return false
	}
	return n.info.State == proto.NodeState_Alive
}

func (n *node) contains(ctx context.Context, role proto.NodeRole) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, r := range n.info.Roles {
		if r == role {
			return true
		}
	}
	return false
}

func (n *node) updateShardCount(delta int32) {
	atomic.AddInt32(&n.shardCount, delta)
}
