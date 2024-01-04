package cluster

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type Allocator interface {
	Put(ctx context.Context, d *disk)
	Alloc(ctx context.Context, args *AllocArgs) ([]*diskInfo, error)
}

type weightSet struct {
	s *setAlloctor
	w int
}

type allocator struct {
	setAlloctors map[proto.SetID]*setAlloctor
}

func NewShardServerAllocator(ctx context.Context) Allocator {
	return &allocator{
		setAlloctors: map[proto.SetID]*setAlloctor{},
	}
}

func (a *allocator) Put(ctx context.Context, n *disk) {
	setID := n.GetNode().SetID
	allocator, ok := a.setAlloctors[setID]
	if !ok {
		allocator = &setAlloctor{
			setID:        setID,
			azAllocators: map[string]*azAllocator{},
		}
		a.setAlloctors[setID] = allocator
	}

	allocator.put(n)
}

func (a *allocator) Alloc(ctx context.Context, args *AllocArgs) ([]*diskInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("set allocator cnt %d", len(a.setAlloctors))

	if args.SetID != 0 {
		targetSet := a.setAlloctors[args.SetID]
		return targetSet.alloc(ctx, args)
	}

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

		allocDisks, err := s.s.alloc(ctx, args)
		if err != nil {
			span.Warnf("alloc from set %d failed, err %s", s.s.setID, err.Error())
			goto Retry
		}

		if len(allocDisks) < args.Count {
			span.Warnf("alloc from set %d not enough, got cnt %d, need %d",
				s.s.setID, len(allocDisks), args.Count)
			goto Retry
		}
		return allocDisks, nil
	}

	span.Warnf("no set can alloc after retry all set, need %d", args.Count)
	return nil, errors.ErrNoAvailableNode
}

type setAlloctor struct {
	setID        proto.SetID
	azAllocators map[string]*azAllocator
	selectIdx    int
	lock         sync.RWMutex

	totalShardCnt uint32
	allocShardCnt uint32
	totalCap      int
	usedCap       int
}

// func (s *setAlloctor) updateStat(totalShardCnt, allocShardCnt, totalCap, usedCap int) {
// 	atomic.StoreUint32(&s.totalShardCnt, uint32(totalShardCnt))
// 	atomic.StoreUint32(&s.allocShardCnt, uint32(allocShardCnt))
// 	s.totalCap = totalCap
// 	s.usedCap = usedCap
// }

func (s *setAlloctor) addShardCnt(cnt int) {
	atomic.AddUint32(&s.allocShardCnt, uint32(cnt))
}

func (s *setAlloctor) put(d *disk) {
	// s.nodes
	az := d.GetNode().Az
	allocator, ok := s.azAllocators[az]
	if !ok {
		allocator = &azAllocator{
			az:       az,
			allNodes: &nodeSet{},
		}
		s.azAllocators[az] = allocator
	}
	allocator.put(d)

	info := d.GetInfo()
	s.totalCap += int(info.Total)
	s.usedCap += int(info.Used)
	s.allocShardCnt += uint32(info.ShardCnt)
	s.totalShardCnt += defaultMaxShardOneDisk
}

func (s *setAlloctor) getWeight() int {
	var w1, w2 int
	if s.totalShardCnt > s.allocShardCnt {
		w1 = int((s.totalShardCnt - s.allocShardCnt) * defaultFactor / s.totalShardCnt)
	}

	if s.usedCap < s.totalCap {
		w2 = (s.totalCap - s.usedCap) * defaultFactor / s.totalCap
	}

	if w1+w2 == 0 {
		return 1
	}

	return w1 + w2
}

func (s *setAlloctor) getSelectIdx() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.selectIdx++

	if s.selectIdx >= len(s.azAllocators) {
		s.selectIdx = 0
	}

	return s.selectIdx
}

func (s *setAlloctor) alloc(ctx context.Context, args *AllocArgs) ([]*diskInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	disks := make([]*disk, 0, args.Count)

	if args.AZ != "" {
		targetAz := s.azAllocators[args.AZ]
		var err error
		disks, err = targetAz.alloc(ctx, args)
		if err != nil {
			return nil, err
		}
	} else {
		azCntMap := map[string]int{}
		azs := make([]*azAllocator, 0, len(s.azAllocators))
		for _, az := range s.azAllocators {
			azs = append(azs, az)
		}

		selectIdx := s.getSelectIdx()
		for idx := 0; idx < args.Count; idx++ {
			azIdx := (idx + selectIdx) % len(s.azAllocators)
			az := azs[azIdx]
			azCntMap[az.az]++
		}

		for name, cnt := range azCntMap {
			az := s.azAllocators[name]
			args.Count = cnt
			azDisks, err := az.alloc(ctx, args)
			if err != nil || len(azDisks) < cnt {
				span.Warnf("alloc from az %s failed, err %v", name, err)
				return nil, errors.ErrNoAvailableNode
			}
			disks = append(disks, azDisks...)
		}
	}

	for _, d := range disks {
		d.AddShardCnt(1)
	}
	s.addShardCnt(args.Count)

	allocDisks := make([]*diskInfo, 0, len(disks))
	for _, d := range disks {
		allocDisks = append(allocDisks, d.GetInfo())
	}

	return allocDisks, nil
}

type azAllocator struct {
	az       string
	allNodes *nodeSet
}

func (a *azAllocator) put(n *disk) {
	a.allNodes.put(n)
}

func (a *azAllocator) alloc(ctx context.Context, args *AllocArgs) ([]*disk, error) {
	span := trace.SpanFromContextSafe(ctx)

	allocNodes := make([]*disk, 0, args.Count)
	excludes := make(map[uint32]bool)
	for _, id := range args.ExcludeDiskIDs {
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

type weightedDisk struct {
	diskID uint32
	weight int32
	disk   *disk
}

type nodeSet struct {
	shardCount int32
	disks      []*disk
}

// TODO add testcase
func (s *nodeSet) alloc(ctx context.Context, count int, excludeDisks map[proto.DiskID]bool, hostWare, rackWare bool) []*disk {
	if count > len(s.disks) {
		return s.disks
	}

	span := trace.SpanFromContext(ctx)

	need := count
	res := make([]*disk, 0, count)
	excludeNodes := make(map[proto.NodeID]bool)

RackRetry:
	weightedNodes := make([]*weightedDisk, 0, len(s.disks))
	totalWeight := int32(0)
	for _, d := range s.disks {
		if !d.CanAlloc() || excludeDisks[d.GetDiskID()] || excludeNodes[d.GetInfo().NodeID] {
			continue
		}
		wn := &weightedDisk{
			diskID: d.GetDiskID(),
			disk:   d,
			weight: d.getWeight(),
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

			need -= 1
			total -= 1
			_totalWeight -= wn.weight
			weightedNodes[i] = weightedNodes[total]

			d := wn.disk
			res = append(res, d)
			node := d.GetNode()
			nodeID := node.ID
			rack := node.Rack
			excludeDisks[d.GetDiskID()] = true
			excludeNodes[nodeID] = true

			if hostWare || rackWare {
				for idx := 0; idx < total; idx++ {
					w1 := weightedNodes[idx]
					d1 := w1.disk

					sameRack := rackWare && d1.GetNode().Rack == rack
					sameNode := hostWare && d1.GetInfo().NodeID == nodeID
					if !sameRack && !sameNode {
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
		span.Info("can't find enough rack, try duplicated rack, set rack false")
		rackWare = false
		goto RackRetry
	}

	return res
}

func (s *nodeSet) put(n *disk) {
	s.disks = append(s.disks, n)
}
