package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/google/uuid"
)

const (
	ConfChangeAddNode        ConfChangeType = 0
	ConfChangeRemoveNode     ConfChangeType = 1
	ConfChangeUpdateNode     ConfChangeType = 2
	ConfChangeAddLearnerNode ConfChangeType = 3
)

var (
	defaultApplyIndexPersistInterval = uint64(1000)
	applyIndexKey                    = []byte("apply_index")
)

type (
	Op             uint32
	ConfChangeType int32

	Snapshot interface {
		Read() ([]byte, error)
		Name() string
		Index() uint64
		Close()
	}
	// The StateMachine interface is supplied by the application to persist/snapshot data of application.
	StateMachine interface {
		Apply(ctx context.Context, module string, op Op, data []byte, index uint64) (result interface{}, err error)
		ApplyMemberChange(cc ConfChange, index uint64) error
		Snapshot() (Snapshot, error)
		ApplySnapshot(st Snapshot) error
		LeaderChange(leader uint64, addr string) error
	}
	Storage interface {
		Get(key []byte) ([]byte, error)
		Set(key, value []byte) error
	}
	Group interface {
		Propose(ctx context.Context, req *ProposeRequest) (*ProposeResponse, error)
		Stat() *Stat
		Start()
		Close()
	}

	raft interface {
		Propose(ctx context.Context, data []byte) error
	}

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
	ConfChange struct {
		Type    ConfChangeType
		NodeID  uint64
		Context []byte
	}
	ProposeRequest struct {
		Module     string
		Op         Op
		Data       []byte
		WithResult bool

		reqId   string
		respKey string
	}
	ProposeResponse struct {
		Data interface{}

		respKey string
	}

	Config struct {
		SM   StateMachine
		Raft raft
	}
)

func (req *ProposeRequest) Marshal() ([]byte, error) {
	return nil, nil
}

func (req *ProposeRequest) Unmarshal(raw []byte) error {
	return nil
}

type group struct {
	applyIndex       uint64
	stableApplyIndex uint64
	sm               StateMachine
	storage          Storage
	raft             raft
	proposeResponses sync.Map
}

func NewRaftGroup(cfg *Config) Group {
	return &group{
		raft: cfg.Raft,
		sm:   cfg.SM,
	}
}

func (r *group) Propose(ctx context.Context, req *ProposeRequest) (*ProposeResponse, error) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		req.reqId = span.TraceID()
	}
	if req.WithResult {
		req.respKey = uuid.NewString()
		r.proposeResponses.Store(req.respKey, struct{}{})
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	// propose to raft
	if err := r.raft.Propose(ctx, data); err != nil {
		return nil, err
	}
	if !req.WithResult {
		return nil, nil
	}

	ret, ok := r.proposeResponses.Load(req.respKey)
	if !ok {
		return nil, errors.New("no result return but with result expected")
	}

	r.proposeResponses.Delete(req.respKey)
	return &ProposeResponse{
		Data:    ret,
		respKey: req.respKey,
	}, nil
}

func (r *group) Start() {
	return
}

func (r *group) Stat() *Stat {
	return nil
}

func (r *group) Close() {
	return
}

func (r *group) Apply(ctx context.Context, data []byte, index uint64) error {
	req := &ProposeRequest{}
	if err := req.Unmarshal(data); err != nil {
		return err
	}
	_, applyCtx := trace.StartSpanFromContextWithTraceID(ctx, "", req.reqId)
	result, err := r.sm.Apply(applyCtx, req.Module, req.Op, req.Data, index)
	if err != nil {
		return err
	}
	if req.WithResult {
		if _, ok := r.proposeResponses.Load(req.respKey); ok {
			r.proposeResponses.Store(req.respKey, result)
		}
	}
	atomic.StoreUint64(&r.applyIndex, index)
	// record apply index timely
	if index < atomic.LoadUint64(&r.stableApplyIndex)+defaultApplyIndexPersistInterval {
		return nil
	}
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, index)
	return r.storage.Set(applyIndexKey, raw)
}

func (r *group) ApplyMemberChange(cc ConfChange, index uint64) error {
	return r.sm.ApplyMemberChange(cc, index)
}

func (r *group) Snapshot() (Snapshot, error) {
	return r.sm.Snapshot()
}

func (r *group) ApplySnapshot(st Snapshot) error {
	return r.sm.ApplySnapshot(st)
}

func (r *group) LeaderChange(leader uint64, addr string) error {
	return r.sm.LeaderChange(leader, addr)
}

type NoopRaft struct{}

func (r *NoopRaft) Propose(ctx context.Context, data []byte) error {
	return nil
}
