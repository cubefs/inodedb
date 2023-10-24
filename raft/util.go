package raft

import (
	"context"
	"math"
	"sync/atomic"
	"time"
)

func newIDGenerator(nodeId uint64, now time.Time) *idGenerator {
	prefix := nodeId << 48
	unixMilli := uint64(now.UnixNano()) / uint64(time.Millisecond/time.Nanosecond)
	suffix := lowBit(unixMilli, 40) << 8
	return &idGenerator{
		prefix: prefix,
		suffix: suffix,
	}
}

// idGenerator generate unique id for propose request
// generated id contains these part of below
// | prefix   | suffix              |
// | 2 bytes  | 5 bytes   | 1 byte  |
// | memberID | timestamp | cnt     |
type idGenerator struct {
	// high order 2 bytes
	prefix uint64
	// low order 6 bytes
	suffix uint64
}

// Next generates a id that is unique.
func (g *idGenerator) Next() uint64 {
	suffix := atomic.AddUint64(&g.suffix, 1)
	return g.prefix | lowBit(suffix, 48)
}

func newNotify() notify {
	// todo: reuse ch with channel pool
	return make(chan proposalResult, 1)
}

type notify chan proposalResult

func (n notify) Notify(ret proposalResult) {
	select {
	case n <- ret:
	default:
	}
}

func (n notify) Wait(ctx context.Context) (ret proposalResult, err error) {
	select {
	case err = <-ctx.Done():
		return
	case ret = <-n:
		return
	}
}

func lowBit(x uint64, n uint) uint64 {
	return x & (math.MaxUint64 >> (64 - n))
}
