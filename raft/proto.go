package raft

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type (
	StateMachine interface {
		Apply(cxt context.Context, ms []ProposalData, index uint64) (rets []interface{}, err error)
		LeaderChange(peerID uint64) error
		ApplyMemberChange(cc *Member, index uint64) error
		Snapshot() Snapshot
		ApplySnapshot(s Snapshot) error
	}
	Storage interface {
		Get(key []byte) (ValGetter, error)
		Iter(prefix []byte) Iterator
		NewBatch() Batch
		Write(b Batch) error
		Put(key, value []byte) error
	}
	Snapshot interface {
		// ReadBatch read batch data for snapshot transmit
		// io.EOF should be return when read end of snapshot
		// Note: it is the responsibility for the caller to close the Batch
		ReadBatch() (Batch, error)
		Term() uint64
		Index() uint64
		Close() error
	}
	AddressResolver interface {
		Resolve(nodeId uint64) (Addr, error)
	}
	Addr interface {
		String() string
	}
	ValGetter interface {
		Data() []byte
		Close()
	}
	Iterator interface {
		SeekForPrev(prev []byte) error
		Next() bool
		Prev() bool
		Err() error
		ValidPrefix() bool
		Key() ValGetter
		Value() ValGetter
		Close()
	}
	Batch interface {
		Put(key, value []byte)
		DeleteRange(start []byte, end []byte)
		Data() []byte
		From(data []byte)
		Close()
	}
)

type (
	Stat struct {
		Id             uint64   `json:"nodeId"`
		Term           uint64   `json:"term"`
		Vote           uint64   `json:"vote"`
		Commit         uint64   `json:"commit"`
		Leader         uint64   `json:"leader"`
		RaftState      string   `json:"raftState"`
		Applied        uint64   `json:"applied"`
		RaftApplied    uint64   `json:"raftApplied"`
		LeadTransferee uint64   `json:"transferee"`
		Peers          []uint64 `json:"peers"`
	}

	ProposalResponse struct {
		Data interface{}
	}

	proposalRequest struct {
		entryType raftpb.EntryType
		data      *ProposalData
	}
	proposalResult struct {
		reply interface{}
		err   error
	}
)
