package catalog

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type routeMgr struct {
	unstableRouteVersion uint64
	stableRouteVersion   uint64
	truncateIntervalNum  uint64
	increments           *routeItemRing
	done                 chan struct{}
	lock                 sync.RWMutex

	storage *storage
}

func (r *routeMgr) GenRouteVersion(ctx context.Context, step uint64) uint64 {
	return atomic.AddUint64(&r.unstableRouteVersion, step)
}

func (r *routeMgr) InsertRouteItems(ctx context.Context, items []*routeItemInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()

	maxStableRouteVersion := uint64(0)
	for _, item := range items {
		r.increments.put(item)
		if item.RouteVersion > maxStableRouteVersion {
			maxStableRouteVersion = item.RouteVersion
		}
	}
	atomic.StoreUint64(&r.stableRouteVersion, maxStableRouteVersion)
}

func (r *routeMgr) GetRouteItems(ctx context.Context, ver uint64) []*routeItemInfo {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.increments.getFrom(ver)
}

func (r *routeMgr) Close() {
	close(r.done)
}

func (r *routeMgr) loop() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	ticker := time.NewTicker(1 * time.Minute)

	for {
		select {
		case <-ticker.C:
			// check route items num, remove oldest route item if exceed the max increment items limit
			item, err := r.storage.GetFirstRouteItem(ctx)
			if err != nil {
				span.Errorf("get first route item failed: %s", err)
				continue
			}

			if item.RouteVersion < atomic.LoadUint64(&r.stableRouteVersion)-r.truncateIntervalNum {
				if err := r.storage.DeleteSpace(ctx, item.RouteVersion); err != nil {
					span.Errorf("delete oldest route items failed: %s", err)
					continue
				}
				span.Infof("delete oldest route items[%d] success", item.RouteVersion)
			}
		case <-r.done:
			return
		}
	}
}

type routeItemRing struct {
	data     []*routeItemInfo
	head     uint32
	tail     uint32
	nextTail uint32
	cap      uint32
	usedCap  uint32
}

func newRouteItemRing(cap uint32) *routeItemRing {
	ring := &routeItemRing{
		data: make([]*routeItemInfo, cap),
		cap:  cap,
	}
	return ring
}

func (r *routeItemRing) put(item *routeItemInfo) {
	r.data[r.nextTail] = item
	r.tail = r.nextTail
	if r.cap == r.usedCap {
		r.head++
		r.head = r.head % r.cap
		r.nextTail++
		r.nextTail = r.nextTail % r.cap
	} else {
		r.nextTail++
		r.nextTail = r.nextTail % r.cap
		r.usedCap++
	}

	if (r.data[r.head].RouteVersion + uint64(r.usedCap)) != (r.data[r.tail].RouteVersion + 1) {
		errMsg := fmt.Sprintf("route cache ring is not consistently, head %v ver: %v, usedCap: %v, tail %v ver: %v",
			r.head, r.data[r.head].RouteVersion, uint64(r.usedCap), r.tail, r.data[r.tail].RouteVersion)
		panic(errMsg)
	}
}

func (r *routeItemRing) getFrom(ver uint64) (ret []*routeItemInfo) {
	if r.getMinVer() > ver || r.getMaxVer() < ver {
		return nil
	}

	headVer := r.data[r.head].RouteVersion
	i := (r.head + uint32(ver-headVer)) % r.cap
	for j := 0; j < int(r.usedCap); j++ {
		ret = append(ret, r.data[i])
		i = (i + 1) % r.cap
		if i == r.nextTail {
			break
		}
	}
	return ret
}

func (r *routeItemRing) getMinVer() uint64 {
	return r.data[r.head].RouteVersion
}

func (r *routeItemRing) getMaxVer() uint64 {
	return r.data[r.tail].RouteVersion
}
