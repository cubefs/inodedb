package cluster

import (
	"context"
	"math/rand"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
)

type Allocator interface {
	Put(ctx context.Context, d *DiskInfo)
	Alloc(ctx context.Context, args *AllocArgs) ([]*diskNode, error)
}

type weightSet struct {
	s *setAlloctor
	w int
}

type allocator struct {
	setAlloctors map[uint32]*setAlloctor
}

func NewShardServerAllocator(ctx context.Context) Allocator {
	return &allocator{
		setAlloctors: map[uint32]*setAlloctor{},
	}
}

func (a *allocator) Put(ctx context.Context, n *DiskInfo) {
	setId := n.node.info.SetId
	allocator, ok := a.setAlloctors[setId]
	if !ok {
		allocator = &setAlloctor{
			setId:        setId,
			azAllocators: map[string]*azAllocator{},
		}
		a.setAlloctors[setId] = allocator
	}

	allocator.put(n)
}

func (a *allocator) Alloc(ctx context.Context, args *AllocArgs) ([]*diskNode, error) {
	span := trace.SpanFromContextSafe(ctx)

	totalWeight := 0
	sets := make([]*weightSet, 0, len(a.setAlloctors))
	for _, set := range a.setAlloctors {
		w := set.getWeight()
		sets = append(sets, &weightSet{
			s: set,
			w: w,
		})
		totalWeight += w
	}

	_totalWeight := totalWeight
	total := len(a.setAlloctors)

Retry:
	randNum := 0
	if _totalWeight > 0 {
		randNum = rand.Intn(_totalWeight)
	}

	for idx := 0; idx < total; idx++ {
		s := sets[idx]
		if s.w < randNum {
			randNum -= s.w
		}

		_totalWeight -= s.w
		total -= 1
		sets[idx] = sets[total]

		allocNodes, err := s.s.alloc(ctx, args)
		if err != nil {
			span.Warnf("alloc from set %d failed, err %s", s.s.setId, err.Error())
			goto Retry
		}
		if len(allocNodes) < args.Count {
			span.Warnf("alloc from set %d not enough, got cnt %d, need %d",
				s.s.setId, len(allocNodes), args.Count)
			goto Retry
		}
		return allocNodes, nil
	}

	span.Warnf("no set can alloc after retry all set, need %d", args.Count)
	return nil, errors.ErrNoAvailableNode
}

func (a *azAllocator) alloc(ctx context.Context, args *AllocArgs) ([]*DiskInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	allocNodes := make([]*DiskInfo, 0, args.Count)
	excludes := make(map[uint32]bool)
	for _, id := range args.ExcludeNodeIds {
		excludes[id] = true
	}

	nodes := a.allNodes.alloc(ctx, args.Count, excludes, args.HostWare, args.RackWare)
	if len(nodes) < args.Count {
		span.Warnf("current az has no enough nodes to alloc, az: %s, count: %d, got %d",
			args.AZ, args.Count, len(nodes))
		return nil, errors.ErrNoAvailableNode
	}

	for _, newNode := range nodes {
		allocNodes = append(allocNodes, newNode)
	}

	return allocNodes, nil
}

type setAlloctor struct {
	setId        uint32
	azAllocators map[string]*azAllocator
	// nodes        *nodeSet
	selIdx int

	totalShardCnt int
	allocShardCnt int
	totalCap      int
	usedCap       int
}

func (s *setAlloctor) updateStat(totalShardCnt, allocShardCnt, totalCap, usedCap int) {
	s.totalShardCnt = totalShardCnt
	s.allocShardCnt = allocShardCnt
	s.totalCap = totalCap
	s.usedCap = usedCap
}

func (s *setAlloctor) addShardCnt(cnt int) {
	s.allocShardCnt += cnt
}

func (s *setAlloctor) put(d *DiskInfo) {
	// s.nodes
	az := d.node.info.Az
	allocator, ok := s.azAllocators[az]
	if !ok {
		s.azAllocators[az] = &azAllocator{
			az:       az,
			allNodes: &nodeSet{},
			// rackNodeSets: make(map[string]*nodeSet),
		}
	}
	allocator.put(d)

	s.totalCap += d.disk.Info.Size()
	s.usedCap += int(d.disk.Info.Used)
	s.allocShardCnt += int(d.disk.Info.ShardCnt)
	s.totalShardCnt += defaultMaxShardOneDisk
}

func (s *setAlloctor) getWeight() int {
	var w1, w2 int
	if s.totalShardCnt > s.allocShardCnt {
		w1 = (s.totalShardCnt - s.allocShardCnt) * defaultFactor / s.totalShardCnt
	}

	if s.usedCap < s.totalCap {
		w2 = (s.totalCap - s.usedCap) * defaultFactor / s.totalCap
	}

	if w1+w2 == 0 {
		return 1
	}

	return w1 + w2
}

func (s *setAlloctor) alloc(ctx context.Context, args *AllocArgs) ([]*diskNode, error) {
	span := trace.SpanFromContextSafe(ctx)

	azCntMap := map[string]int{}

	azs := make([]*azAllocator, 0, len(s.azAllocators))
	for _, az := range s.azAllocators {
		azs = append(azs, az)
	}

	for idx := 0; idx < args.Count; idx++ {
		azIdx := (idx + s.selIdx) % len(s.azAllocators)
		az := azs[azIdx]
		azCntMap[az.az]++
	}

	s.selIdx++

	nodeInfos := make([]*DiskInfo, 0, args.Count)

	for name, cnt := range azCntMap {
		az := s.azAllocators[name]
		args.Count = cnt
		azNodes, err := az.alloc(ctx, args)
		if err != nil || len(azNodes) < cnt {
			span.Warnf("alloc from az %s failed, err %v", name, err)
			return nil, errors.ErrNoAvailableNode
		}
		nodeInfos = append(nodeInfos, azNodes...)
	}

	for _, d := range nodeInfos {
		d.AddShardCnt(1)
	}

	s.addShardCnt(args.Count)

	allocNodes := make([]*diskNode, 0, len(nodeInfos))
	for _, info := range nodeInfos {
		allocNodes = append(allocNodes, info.disk)
	}
	return allocNodes, nil
}

type azAllocator struct {
	az       string
	allNodes *nodeSet
}

func (a *azAllocator) put(n *DiskInfo) {
	a.allNodes.put(n)
}

type nodeSet struct {
	shardCount int32

	nodes []*DiskInfo
}

type weightedNode struct {
	diskId uint32
	weight int32
	n      *DiskInfo
}

func (s *nodeSet) alloc(ctx context.Context, count int, excludes map[uint32]bool, hostWare, rackWare bool) []*DiskInfo {
	if count > len(s.nodes) {
		return s.nodes
	}

	span := trace.SpanFromContext(ctx)

	need := count
	res := make([]*DiskInfo, 0, count)
	excludesDiskId := make(map[uint32]bool)

RackRetry:
	weightedNodes := make([]*weightedNode, 0, len(s.nodes))
	totalWeight := int32(0)
	for _, n := range s.nodes {
		if !n.CanAlloc() || excludes[n.GetNodeId()] || excludesDiskId[n.GetDiskId()] {
			continue
		}
		wn := &weightedNode{
			diskId: n.GetDiskId(),
			n:      n,
			weight: n.getWeight(),
		}
		weightedNodes = append(weightedNodes, wn)
		totalWeight += wn.weight
	}

	if len(weightedNodes) == 0 {
		return res
	}

	total := len(weightedNodes)
	_totalWeight := totalWeight

RETRY:
	for need > 0 && total > 0 {
		randNum := int32(0)
		if totalWeight > 0 {
			randNum = rand.Int31n(_totalWeight)
		}

		for i := 0; i < total; i++ {
			wn := weightedNodes[i]
			if wn.weight <= randNum {
				randNum -= wn.weight
				continue
			}

			res = append(res, wn.n)
			need -= 1
			total -= 1
			_totalWeight -= wn.weight
			weightedNodes[i] = weightedNodes[total]
			excludesDiskId[wn.n.GetDiskId()] = true

			nodeId := wn.n.GetNodeId()
			rack := wn.n.GetRack()

			if hostWare || rackWare {
				for idx := 0; idx < total; idx++ {
					w1 := weightedNodes[idx]

					if hostWare && w1.n.GetNodeId() != nodeId {
						continue
					}

					if rackWare && w1.n.GetRack() != rack {
						continue
					}

					total -= 1
					_totalWeight -= w1.weight
					weightedNodes[idx] = weightedNodes[total]
				}
			}

			goto RETRY
		}
	}

	if need > 0 && rackWare {
		span.Info("can't find enough rack, try duplicated rack")
		goto RackRetry
	}

	return res
}

func (s *nodeSet) put(n *DiskInfo) {
	s.nodes = append(s.nodes, n)
}
