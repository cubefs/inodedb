package catalog

import (
	"context"
	"sync/atomic"

	"github.com/cubefs/inodedb/master/idgenerator"
)

const (
	idName = "route"
)

type routeMgr struct {
	stableRouteVersion   uint64
	unstableRouteVersion uint64

	idGenerator idgenerator.IDGenerator
}

func (r *routeMgr) GenRouteVersion(ctx context.Context, step int) (base, new uint64, err error) {
	base, new, err = r.idGenerator.Alloc(ctx, idName, step)
	return
}

func (r *routeMgr) SaveRouteVersion(ver uint64) {
	for {
		old := atomic.LoadUint64(&r.stableRouteVersion)
		if old >= ver {
			return
		}

		if atomic.CompareAndSwapUint64(&r.stableRouteVersion, old, ver) {
			return
		}
	}
}
