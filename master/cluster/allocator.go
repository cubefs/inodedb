package cluster

import (
	"context"
	"math/rand"
	"sort"
	"sync"

	"github.com/cubefs/inodedb/errors"
)

type AllocMgr interface {
	Put(ctx context.Context, n *node)
	Remove(ctx context.Context, nodeId uint32, az string)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
}

type allocMgr struct {
	allocator sync.Map

	loadThreshold int32
	loadEnable    bool

	lock sync.RWMutex
}

func (a *allocMgr) Put(ctx context.Context, n *node) {
	value, _ := a.allocator.LoadOrStore(n.info.Az, &nodeSet{
		loadEnable:    a.loadEnable,
		loadThreshold: a.loadThreshold,
	})
	set := value.(*nodeSet)
	set.Put(n)
}

func (a *allocMgr) Remove(ctx context.Context, nodeId uint32, az string) {
	if value, loaded := a.allocator.Load(az); loaded {
		set := value.(*nodeSet)
		set.Delete(nodeId)
	}
}

func (a *allocMgr) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	allocNodes := make([]*NodeInfo, 0, args.Count)
	if args.Az != "" {
		return a.allocByAz(ctx, args)
	}
	selectedId := make(map[uint32]struct{}, args.Count)
	a.allocator.Range(func(key, value interface{}) bool {
		set := value.(*nodeSet)
		if alloc := set.Alloc(); alloc != nil {
			if _, ok := selectedId[alloc.nodeId]; ok {
				return true
			}
			allocNodes = append(allocNodes, alloc.info)
			selectedId[alloc.nodeId] = struct{}{}
			if len(allocNodes) == args.Count {
				return false
			}
		}
		return true
	})
	if len(allocNodes) < args.Count {
		return nil, errors.ErrNoAvailableNode
	}
	return allocNodes, nil
}

func (a *allocMgr) allocByAz(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	value, ok := a.allocator.Load(args.Az)
	if !ok {
		return nil, errors.ErrAzNotExist
	}
	set := value.(*nodeSet)
	res := make([]*NodeInfo, 0, args.Count)
	selectedId := make(map[uint32]struct{}, args.Count)

	for i := 0; i < args.Count; i++ {
		alloc := set.Alloc()
		if alloc == nil {
			return res, errors.ErrNoAvailableNode
		}
		if _, ok := selectedId[alloc.nodeId]; ok {
			continue
		}
		selectedId[alloc.nodeId] = struct{}{}
		res = append(res, alloc.info)
	}

	return res, nil
}

type nodeSet struct {
	loadThreshold int32
	loadEnable    bool

	nodes []*node
	lock  sync.RWMutex
}

func (s *nodeSet) Alloc() *node {
	s.lock.RLock()
	total := len(s.nodes)
	nodes := make([]*node, total)
	copy(nodes, s.nodes)
	s.lock.RUnlock()

	if s.loadEnable {
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].load < nodes[j].load
		})
	}

	randTotal := total
	var res *node

	for i := 0; i < total; i++ {
		res = func() *node {
			randNum := rand.Intn(randTotal)
			defer func() {
				nodes[randTotal-1], nodes[randNum] = nodes[randNum], nodes[randTotal-1]
				randTotal--
			}()
			n := nodes[randNum]
			if s.loadEnable && s.loadThreshold < n.load {
				return nil
			}
			if !n.isAvailable() {
				return nil
			}
			return n
		}()
		if res != nil {
			break
		}
	}
	return res
}

func (s *nodeSet) Get(id uint32) (*node, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	i, ok := search(s.nodes, id)
	if !ok {
		return nil, false
	}
	return s.nodes[i], true
}

func (s *nodeSet) Put(n *node) {
	s.lock.Lock()
	defer s.lock.Unlock()
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

func (s *nodeSet) Delete(id uint32) {
	s.lock.Lock()
	defer s.lock.Unlock()
	i, ok := search(s.nodes, id)
	if ok {
		copy(s.nodes[i:], s.nodes[i+1:])
		s.nodes = s.nodes[:len(s.nodes)-1]
	}
}

func (s *nodeSet) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.nodes)
}

func (s *nodeSet) List() (nodes []*node) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	nodes = s.nodes
	return nodes
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
