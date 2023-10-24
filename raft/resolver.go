package raft

import "sync"

// todo: as we store raft group member info into storage,
// shall we implements AddressResolver by raft group?
type cacheAddressResolver struct {
	m        sync.Map
	resolver AddressResolver
}

func (r *cacheAddressResolver) Resolve(nodeId uint64) (Addr, error) {
	if v, ok := r.m.Load(nodeId); ok {
		return v.(Addr), nil
	}

	addr, err := r.resolver.Resolve(nodeId)
	if err != nil {
		return nil, err
	}
	r.m.Store(nodeId, addr)
	return addr, nil
}
