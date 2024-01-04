package base

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/raft"
)

type Applier interface {
	Apply(cxt context.Context, pd []raft.ProposalData) (rets []interface{}, err error)
	LeaderChange(leader uint64) error
	GetCF() []kvstore.CF
	GetModule() string
}

type RaftGroup interface {
	raft.Group
	GetID() uint64
}

type raftServer struct {
	id uint64
	raft.Group
}

func (r *raftServer) GetID() uint64 {
	return r.id
}
