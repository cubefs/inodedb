package cluster

import (
	"context"
	"math/rand"
	"sort"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
)

type AllocMgr interface {
	Put(ctx context.Context, az string, n *node)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
}

type allocMgr struct {
	allocator     map[string]*nodeSet
	loadThreshold int32
}

type AllocConfig struct {
	NodeLoadThreshold int32 `json:"node_load_threshold"`
}

func NewAllocMgr(ctx context.Context, cfg *AllocConfig) AllocMgr {
	return &allocMgr{
		loadThreshold: cfg.NodeLoadThreshold,
	}
}

func (a *allocMgr) Put(ctx context.Context, az string, n *node) {
	if _, hit := a.allocator[az]; !hit {
		a.allocator[az] = &nodeSet{
			loadThreshold: a.loadThreshold,
		}
	}
	a.allocator[az].put(n)
}

func (a *allocMgr) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	allocNodes := make([]*NodeInfo, 0, args.Count)

	allocated, err := a.allocInAz(ctx, args)
	if err != nil {
		span.Warnf("alloc nodes failed from az[%s], err: %s", args.Az, err)
		return allocated, err
	}
	return allocNodes, nil
}

func (a *allocMgr) allocInAz(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	set := a.allocator[args.Az]

	res := make([]*NodeInfo, 0, args.Count)
	allocated := set.alloc(args.Count)
	for _, newNode := range allocated {
		newNode.lock.RLock()
		info := newNode.info.Clone()
		newNode.lock.RUnlock()
		res = append(res, info)
	}

	if len(res) < args.Count {
		return res, errors.ErrNoAvailableNode
	}
	return res, nil
}

type nodeSet struct {
	loadThreshold int32

	nodes []*node
}

func (s *nodeSet) alloc(count int) []*node {
	if len(s.nodes) < count {
		return s.nodes
	}

	type loadNode struct {
		nodeId uint32
		load   int32
	}
	var nodes []*loadNode
	need := count
	res := make([]*node, 0, count)

	for _, n := range s.nodes {
		if !n.isAvailable() {
			return nil
		}
		nodes = append(nodes, &loadNode{
			nodeId: n.nodeId,
			load:   atomic.LoadInt32(&n.load),
		})
	}

	if s.isEnableNodeLoad() {
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].load < nodes[j].load
		})
	}

	total := len(nodes)
	randTotal := total/2 + 1
	for i := 0; i < total; i++ {
		get := func() *node {
			randNum := rand.Intn(randTotal)
			defer func() {
				nodes[randTotal-1], nodes[randNum] = nodes[randNum], nodes[randTotal-1]
				randTotal--
			}()
			n := nodes[randNum]
			if s.isEnableNodeLoad() && s.loadThreshold < n.load {
				return nil
			}
			if newNode, hit := s.get(n.nodeId); hit {
				return newNode
			}
			return nil
		}()
		if get != nil {
			atomic.AddInt32(&get.load, 1)
			res = append(res, get)
			need--
		}
		if need == 0 {
			break
		}
	}
	return res
}

func (s *nodeSet) get(id uint32) (*node, bool) {
	i, ok := search(s.nodes, id)
	if !ok {
		return nil, false
	}
	return s.nodes[i], true
}

func (s *nodeSet) put(n *node) {
	idx, ok := search(s.nodes, n.nodeId)
	if !ok {
		s.nodes = append(s.nodes, n)
		if idx == len(s.nodes)-1 {
			return
		}
		copy(s.nodes[idx+1:], s.nodes[idx:len(s.nodes)-1])
		s.nodes[idx] = n
	}
}

func (s *nodeSet) delete(id uint32) {
	i, ok := search(s.nodes, id)
	if ok {
		copy(s.nodes[i:], s.nodes[i+1:])
		s.nodes = s.nodes[:len(s.nodes)-1]
	}
}

func (s *nodeSet) Len() int {
	return len(s.nodes)
}

func (s *nodeSet) List() (nodes []*node) {
	nodes = s.nodes
	return nodes
}

func (s *nodeSet) isEnableNodeLoad() bool {
	return s.loadThreshold > 0
}

func search(nodes []*node, nodeId uint32) (int, bool) {
	idx := sort.Search(len(nodes), func(i int) bool {
		return nodes[i].nodeId >= nodeId
	})
	if idx == len(nodes) || nodes[idx].nodeId != nodeId {
		return idx, false
	}
	return idx, true
}
