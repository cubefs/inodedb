package base

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/raft"
)

type Applier interface {
	Apply(ctx context.Context, pds []ApplyReq) (rets []ApplyRet, err error)
	LeaderChange(leader uint64) error
	GetCF() []kvstore.CF
	GetModule() string
}

type RaftServer interface {
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

type ApplyReq struct {
	Data raft.ProposalData
	Idx  int
}

type ApplyRet struct {
	Ret interface{}
	Idx int
}
