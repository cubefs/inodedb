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
	Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error)
}

type allocator struct {
	azAllocators map[string]*azAllocator
}

func NewShardServerAllocator(ctx context.Context) Allocator {
	return &allocator{}
}

func (a *allocator) Put(ctx context.Context, n *node) {
	az := n.GetInfo().Az
	if _, hit := a.azAllocators[az]; !hit {
		a.azAllocators[az] = &azAllocator{
			allNodes:     &nodeSet{},
			rackNodeSets: make(map[string]*nodeSet),
		}
	}
	a.azAllocators[az].put(n)
}

func (a *allocator) Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if args.RackWare {
		return a.allocFromRack(ctx, args)
	}

	allocNodes := make([]*nodeInfo, 0, args.Count)
	azAllocator := a.azAllocators[args.Az]
	excludes := make(map[uint32]bool)
	for _, id := range args.ExcludeNodeIds {
		excludes[id] = true
	}

	nodes := azAllocator.allNodes.alloc(args.Count, excludes)
	for _, newNode := range nodes {
		allocNodes = append(allocNodes, newNode.GetInfo())
	}

	if len(allocNodes) < args.Count {
		span.Warnf("alloc nodes failed from az[%s], need: %d, get: %d", args.Az, args.Count, len(allocNodes))
		return allocNodes, errors.ErrNoAvailableNode
	}
	return allocNodes, nil
}

type weightedNodeSet struct {
	weight int32
	rack   string
	num    int
	set    *nodeSet
}

func (a *allocator) allocFromRack(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	allocNodes := make([]*nodeInfo, 0, args.Count)
	azAllocator := a.azAllocators[args.Az]
	rackMap := make(map[string]int, len(a.azAllocators))
	totalWeight := int32(0)

	excludes := make(map[uint32]bool)
	for _, id := range args.ExcludeNodeIds {
		excludes[id] = true
	}

	weightedRackNodeSets := make([]*weightedNodeSet, 0, len(azAllocator.rackNodeSets))
	for rack, set := range azAllocator.rackNodeSets {
		wn := &weightedNodeSet{
			rack:   rack,
			num:    len(set.nodes),
			weight: azAllocator.getFactor(set),
		}
		rackMap[rack] = 0
		weightedRackNodeSets = append(weightedRackNodeSets, wn)
		totalWeight += wn.weight
	}

	chosenIdx := 0
	need := args.Count
	_totalWeight := totalWeight
	rackNum := len(weightedRackNodeSets)

RETRY:
	randNum := int32(0)
	if _totalWeight > 0 {
		randNum = rand.Int31n(_totalWeight)
	}
	for i := chosenIdx; i < rackNum; i++ {
		if randNum <= weightedRackNodeSets[i].weight {
			if rackMap[weightedRackNodeSets[i].rack] >= weightedRackNodeSets[i].num {
				continue
			}
			rackMap[weightedRackNodeSets[i].rack] += 1
			_totalWeight -= weightedRackNodeSets[i].weight
			weightedRackNodeSets[i].weight -= 1
			weightedRackNodeSets[chosenIdx], weightedRackNodeSets[i] = weightedRackNodeSets[i], weightedRackNodeSets[chosenIdx]
			chosenIdx++
			need--
			if need == 0 {
				break
			}
			goto RETRY
		}
		randNum -= weightedRackNodeSets[i].weight
	}

	if need > 0 {
		span.Info("can't find enough rack, try duplicated rack")
		chosenIdx = 0
		_totalWeight = totalWeight - int32(args.Count-need)
		goto RETRY
	}

	for rack, num := range rackMap {
		nodes := azAllocator.rackNodeSets[rack].alloc(num, excludes)
		// update total shard count of each rack
		azAllocator.rackNodeSets[rack].updateTotalShardCount(int32(num))
		for _, node := range nodes {
			allocNodes = append(allocNodes, node.GetInfo())
		}
	}
	// update total shard count of az
	azAllocator.allNodes.updateTotalShardCount(int32(len(allocNodes)))

	return allocNodes, nil
}

type azAllocator struct {
	rackNodeSets map[string]*nodeSet

	allNodes *nodeSet
}

func (a *azAllocator) put(n *node) {

	a.allNodes.put(n)

	n.lock.RLock()
	rack := n.info.Rack
	n.lock.RUnlock()
	if _, ok := a.rackNodeSets[rack]; !ok {
		a.rackNodeSets[rack] = &nodeSet{}
	}
	a.rackNodeSets[rack].put(n)
}

func (a *azAllocator) getFactor(set *nodeSet) int32 {
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
	weightedNodes := make([]*weightedNode, 0, len(s.nodes))
	need := count
	totalWeight := int32(0)
	res := make([]*node, 0, count)

	for _, n := range s.nodes {
		if !n.IsAvailable() || excludes[n.nodeId] {
			continue
		}
		wn := &weightedNode{
			nodeId: n.nodeId,
			n:      n,
			weight: s.getFactor(n),
		}
		weightedNodes = append(weightedNodes, wn)
		totalWeight += wn.weight
	}

	chosenIdx := 0
	_totalWeight := totalWeight
	total := len(weightedNodes)

RETRY:
	for need > 0 {
		randNum := int32(0)
		if _totalWeight > 0 {
			randNum = rand.Int31n(_totalWeight)
		}
		for i := chosenIdx; i < total; i++ {
			wn := weightedNodes[i]
			if wn.weight >= randNum {
				_totalWeight = _totalWeight - wn.weight
				res = append(res, wn.n)
				weightedNodes[chosenIdx], wn = wn, weightedNodes[chosenIdx]
				need -= 1
				chosenIdx += 1
				goto RETRY
			}
			randNum -= wn.weight
		}
	}
	for _, node := range res {
		node.UpdateShardCount(1)
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
