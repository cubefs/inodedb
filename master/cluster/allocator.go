package cluster

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
)

type AllocMgr interface {
	Put(ctx context.Context, az string, n *node)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
}

type allocMgr struct {
	allocator map[string]*nodeSet
}

func NewAllocMgr(ctx context.Context) AllocMgr {
	return &allocMgr{}
}

func (a *allocMgr) Put(ctx context.Context, az string, n *node) {
	if _, hit := a.allocator[az]; !hit {
		a.allocator[az] = &nodeSet{}
	}
	a.allocator[az].put(n)
}

func (a *allocMgr) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	allocNodes := make([]*NodeInfo, 0, args.Count)

	set := a.allocator[args.Az]

	allocated := set.alloc(args.Count)
	for _, newNode := range allocated {
		newNode.lock.RLock()
		info := newNode.info.Clone()
		newNode.lock.RUnlock()
		allocNodes = append(allocNodes, info)
	}
	if len(allocNodes) < args.Count {
		span.Warnf("alloc nodes failed from az[%s], err: %s", args.Az, errors.ErrNoAvailableNode)
		return allocNodes, errors.ErrNoAvailableNode
	}
	return allocNodes, nil
}

type nodeSet struct {
	shardCount int32

	nodes []*node
}

type weightedNode struct {
	nodeId uint32
	weight int32
	n      *node
}

func (s *nodeSet) alloc(count int) []*node {
	if count >= len(s.nodes) {
		return s.nodes
	}
	nodes := make([]*weightedNode, 0, len(s.nodes))
	need := count
	totalWeight := int32(0)
	res := make([]*node, 0, count)

	for _, n := range s.nodes {
		if !n.isAvailable() {
			continue
		}
		l := &weightedNode{
			nodeId: n.nodeId,
			n:      n,
			weight: s.getFactor(n),
		}
		nodes = append(nodes, l)
		totalWeight += l.weight
	}

	selected := make(map[uint32]struct{}, count)
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	chosenIdx := 0
	_totalWeight := totalWeight
	total := len(nodes)

RETRY:
	for need > 0 {
		randNum := int32(0)
		if _totalWeight > 0 {
			randNum = rand.Int31n(totalWeight)
		}
		for i := chosenIdx; i < total; i++ {
			if _, ok := selected[nodes[i].nodeId]; ok {
				continue
			}
			get := func() *node {
				if nodes[i].weight >= randNum {
					_totalWeight = _totalWeight - nodes[i].weight
					return nodes[i].n
				}
				randNum -= nodes[i].weight
				return nil
			}()
			if get != nil {
				res = append(res, get)
				selected[get.nodeId] = struct{}{}
				nodes[chosenIdx], nodes[i] = nodes[i], nodes[chosenIdx]
				need -= 1
				chosenIdx += 1

				goto RETRY
			}
		}
	}
	for _, newNode := range res {
		newNode.updateShardCount(1)
	}
	s.updateTotalShardCount(int32(len(res)))
	return res
}

func (s *nodeSet) put(n *node) {
	s.nodes = append(s.nodes, n)
}

func (s *nodeSet) updateTotalShardCount(delta int32) {
	atomic.AddInt32(&s.shardCount, delta)
}

func (s *nodeSet) getFactor(n *node) int32 {
	shardCount := atomic.LoadInt32(&n.shardCount)
	total := atomic.LoadInt32(&s.shardCount)

	return total - shardCount
}
