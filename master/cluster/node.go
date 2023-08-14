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

	load             int32
	heartbeatTimeout int
	expires          time.Time
	lock             sync.RWMutex
}

func (n *node) handleHeartbeat(ctx context.Context) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.expires = time.Now().Add(time.Duration(n.heartbeatTimeout) * time.Second)

	if n.info.State != proto.NodeState_Alive {
		n.info.State = proto.NodeState_Alive
	}
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

func (n *node) updateLoad(ctx context.Context, delta int32) int32 {
	return atomic.AddInt32(&n.load, delta)
}

func (n *node) contains(ctx context.Context, role proto.NodeRole) bool {
	for _, r := range n.info.Roles {
		if r == role {
			return true
		}
	}
	return false
}
