package catalog

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/store"
)

const (
	defaultNumPerSlice = 10000
	defaultSizeReceive = 128
)

type Router interface {
	AddRouter(ctx context.Context, item *ChangeItem) error
	ListAll(ctx context.Context) []*ChangeItem
	ListFrom(ctx context.Context, start uint64) ([]*ChangeItem, error)
}

type router struct {
	firstVersion uint64
	lastVersion  uint64

	allItems *shardedItems
	store    *store.Store

	closeChan chan struct{}

	lock sync.RWMutex
}

func NewRouter() Router {
	return &router{
		allItems:  newShardedItems(),
		closeChan: make(chan struct{}),
	}
}

func (r *router) AddRouter(ctx context.Context, item *ChangeItem) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("add router")

	if item.RouteVersion != 0 {
		version := r.generateVersion()
		item.RouteVersion = version
	}

	// todo raft propose

	// todo  move to raft apply process
	kvStore := r.store.KVStore()
	value, err := item.Value()
	if err != nil {
		return err
	}
	err = kvStore.SetRaw(ctx, routerCF, item.Key(), value, nil)
	if err != nil {
		return err
	}
	r.allItems.putItems(item)

	return nil
}

func (r *router) ListAll(ctx context.Context) []*ChangeItem {
	res, err := r.ListFrom(ctx, r.firstVersion)
	if err != nil {
		return nil
	}
	return res
}

func (r *router) ListFrom(ctx context.Context, start uint64) ([]*ChangeItem, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if start > r.lastVersion || start < r.firstVersion {
		return nil, errors.ErrRouteVersionConflict
	}
	res := r.allItems.listFrom(start, r.lastVersion-start+1)
	return res, nil
}

func (r *router) generateVersion() uint64 {
	return atomic.AddUint64(&r.lastVersion, 1)
}

func (r *router) loop() {
	go func() {
		for {
			select {
			case <-r.closeChan:
				return
			}
		}
	}()
}

type items struct {
	changes []*ChangeItem
	lock    sync.RWMutex
}

func (s *items) Get(version uint64) (*ChangeItem, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	i, ok := search(s.changes, version)
	if !ok {
		return nil, false
	}
	return s.changes[i], true
}

func (s *items) Put(item *ChangeItem) {
	s.lock.Lock()
	defer s.lock.Unlock()
	idx, ok := search(s.changes, item.RouteVersion)
	if ok {
		return
	}
	s.changes = append(s.changes, item)
	if idx == len(s.changes)-1 {
		s.lock.Unlock()
		return
	}
	copy(s.changes[idx+1:], s.changes[idx:len(s.changes)-1])
	s.changes[idx] = item
}

func (s *items) Delete(version uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	i, ok := search(s.changes, version)
	if ok {
		copy(s.changes[i:], s.changes[i+1:])
		s.changes = s.changes[:len(s.changes)-1]
	}
}

func (s *items) List() (changes []*ChangeItem) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	changes = s.changes[:]
	return changes
}

func (s *items) CopyFrom(start uint64) []*ChangeItem {
	s.lock.RLock()
	defer s.lock.RUnlock()
	idx, b := search(s.changes, start)
	if !b {
		return nil
	}
	return s.changes[idx:]
}

func search(changes []*ChangeItem, version uint64) (int, bool) {
	idx := sort.Search(len(changes), func(i int) bool {
		return changes[i].RouteVersion >= version
	})
	if idx == len(changes) || changes[idx].RouteVersion != version {
		return idx, false
	}
	return idx, true
}

// concurrentShards is an effective data struct
type shardedItems struct {
	num  uint64
	m    []*items
	lock sync.RWMutex
}

func newShardedItems() *shardedItems {
	m := &shardedItems{
		num: defaultNumPerSlice,
		m:   make([]*items, 0, 1),
	}
	return m
}

// get shard from concurrentShards
func (s *shardedItems) getItems(version uint64) (*ChangeItem, bool) {
	idx := version / s.num
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.m[idx].Get(version)
}

// put new shard into concurrentShards
func (s *shardedItems) putItems(v *ChangeItem) {
	idx := v.RouteVersion / s.num
	s.lock.Lock()
	defer s.lock.Unlock()
	if idx == uint64(len(s.m)) {
		s.m = append(s.m, &items{
			changes: make([]*ChangeItem, 0, s.num),
		})
	}
	s.m[idx].Put(v)
}

func (s *shardedItems) listFrom(start, need uint64) []*ChangeItem {
	s.lock.RLock()
	defer s.lock.RUnlock()
	res := make([]*ChangeItem, need)
	startIdx := start / s.num
	for i := startIdx; i < uint64(len(s.m)); i++ {
		res = append(res, s.m[startIdx].CopyFrom(start)...)
	}
	return res
}
