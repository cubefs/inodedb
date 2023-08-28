package cluster

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
)

type Allocator interface {
	Put(ctx context.Context, n *node)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
}

type allocator struct {
	allocator map[string]*azStorage
}

func NewAllocator(ctx context.Context) Allocator {
	return &allocator{}
}

func (a *allocator) Put(ctx context.Context, n *node) {
	az := n.GetInfo().Az
	if _, hit := a.allocator[az]; !hit {
		a.allocator[az] = &azStorage{
			allNodes:    &nodeSet{},
			rackStorage: make(map[string]*nodeSet),
		}
	}
	a.allocator[az].put(n)
}

func (a *allocator) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if args.RackWare {
		return a.allocFromRack(ctx, args)
	}

	allocNodes := make([]*NodeInfo, 0, args.Count)
	stg := a.allocator[args.Az]
	selected := make(map[uint32]bool)

	allocated := stg.allNodes.alloc(args.Count, selected)
	for _, newNode := range allocated {
		allocNodes = append(allocNodes, newNode.GetInfo())
	}

	if len(allocNodes) < args.Count {
		span.Warnf("alloc nodes failed from az[%s], need: %d, get: %d", args.Az, args.Count, len(allocNodes))
		return allocNodes, errors.ErrNoAvailableNode
	}
	return allocNodes, nil
}

func (a *allocator) allocFromRack(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	allocNodes := make([]*NodeInfo, 0, args.Count)
	selectedNodes := make(map[uint32]bool, args.Count)
	failedRack := make(map[string]bool)
	as := a.allocator[args.Az]
	totalWeight := int32(0)

	type weightedStorage struct {
		weight  int32
		rack    string
		storage *nodeSet
	}
	rackStorages := make([]*weightedStorage, 0, len(as.rackStorage))
	for rack, stg := range as.rackStorage {
		ws := &weightedStorage{
			rack:    rack,
			weight:  as.getFactor(stg),
			storage: stg,
		}
		rackStorages = append(rackStorages, ws)
		totalWeight += ws.weight
	}
	rand.Shuffle(len(rackStorages), func(i, j int) {
		rackStorages[i], rackStorages[j] = rackStorages[j], rackStorages[i]
	})

	chosenIdx := 0
	_totalWeight := totalWeight
	rackNum := len(rackStorages)

RETRY:
	randNum := int32(0)
	if _totalWeight > 0 {
		randNum = rand.Int31n(_totalWeight)
	}
	for i := chosenIdx; i < rackNum; i++ {
		if randNum <= rackStorages[i].weight {
			if failedRack[rackStorages[i].rack] {
				continue
			}
			set := rackStorages[i].storage
			res := set.alloc(1, selectedNodes)
			if len(res) > 0 {
				newNode := res[0]
				allocNodes = append(allocNodes, newNode.GetInfo())
				selectedNodes[newNode.nodeId] = true
				_totalWeight -= rackStorages[i].weight
				rackStorages[i].weight -= 1
				set.updateTotalShardCount(1)
				rackStorages[chosenIdx], rackStorages[i] = rackStorages[i], rackStorages[chosenIdx]
				chosenIdx++
				goto RETRY
			}
			// if alloc failed, need not retry this rack
			failedRack[rackStorages[i].rack] = true
			span.Warnf("alloc nodes failed from rack[%s], err: %s", rackStorages[i].rack, errors.ErrNoAvailableNode)
			continue
		}
		randNum -= rackStorages[i].weight
	}

	if len(allocNodes) < args.Count && len(failedRack) < rackNum {
		span.Info("can't find enough rack, try duplicated rack")
		chosenIdx = 0
		_totalWeight = totalWeight - int32(len(allocNodes))
		goto RETRY
	}

	as.allNodes.updateTotalShardCount(int32(len(allocNodes)))
	if len(allocNodes) < args.Count {
		return allocNodes, errors.ErrNoAvailableNode
	}

	return allocNodes, nil
}

type azStorage struct {
	rackStorage map[string]*nodeSet

	allNodes *nodeSet
}

func (a *azStorage) put(n *node) {

	a.allNodes.put(n)

	n.lock.RLock()
	rack := n.info.Rack
	n.lock.RUnlock()
	if _, ok := a.rackStorage[rack]; !ok {
		a.rackStorage[rack] = &nodeSet{}
	}
	a.rackStorage[rack].put(n)
}

func (a *azStorage) getFactor(set *nodeSet) int32 {
	total := a.allNodes.getShardCount()
	rackShardCount := set.getShardCount()

	return total - rackShardCount
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

func (s *nodeSet) alloc(count int, excludes map[uint32]bool) []*node {
	if count >= len(s.nodes) {
		return s.nodes
	}
	nodes := make([]*weightedNode, 0, len(s.nodes))
	need := count
	totalWeight := int32(0)
	res := make([]*node, 0, count)

	for _, n := range s.nodes {
		if !n.IsAvailable() || excludes[n.nodeId] {
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
			randNum = rand.Int31n(_totalWeight)
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
		newNode.UpdateShardCount(1)
	}
	return res
}

func (s *nodeSet) put(n *node) {
	s.nodes = append(s.nodes, n)
	s.updateTotalShardCount(n.GetShardCount())
}

func (s *nodeSet) updateTotalShardCount(delta int32) {
	atomic.AddInt32(&s.shardCount, delta)
}

func (s *nodeSet) getShardCount() int32 {
	return atomic.LoadInt32(&s.shardCount)
}

func (s *nodeSet) getFactor(n *node) int32 {
	shardCount := n.GetShardCount()
	total := atomic.LoadInt32(&s.shardCount)

	return total - shardCount
}
