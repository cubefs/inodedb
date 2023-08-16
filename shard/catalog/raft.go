package catalog

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/google/uuid"
)

const (
	RaftOpInsertItem RaftOp = iota + 1
	RaftOpUpdateItem
	RaftOpDeleteItem
	RaftOpLinkItem
	RaftOpUnlinkItem
)

const (
	ConfChangeAddNode        RaftConfChangeType = 0
	ConfChangeRemoveNode     RaftConfChangeType = 1
	ConfChangeUpdateNode     RaftConfChangeType = 2
	ConfChangeAddLearnerNode RaftConfChangeType = 3
)

var (
	defaultApplyIndexPersistInterval = uint64(1000)
	applyIndexKey                    = []byte("apply_index")
)

type (
	RaftOp             uint32
	RaftConfChangeType int32

	RaftSnapshot interface {
		Read() ([]byte, error)
		Name() string
		Index() uint64
		Close()
	}
	// The RaftStateMachine interface is supplied by the application to persist/snapshot data of application.
	RaftStateMachine interface {
		Apply(ctx context.Context, op RaftOp, data []byte, index uint64) (result interface{}, err error)
		ApplyMemberChange(cc RaftConfChange, index uint64) error
		Snapshot() (RaftSnapshot, error)
		ApplySnapshot(st RaftSnapshot) error
		LeaderChange(leader uint64, addr string) error
	}
	RaftStorage interface {
		Get(key []byte) ([]byte, error)
		Set(key, value []byte) error
	}
	Raft interface {
		Propose(ctx context.Context, data []byte) error
	}

	RaftConfChange struct {
		Type    RaftConfChangeType
		NodeID  uint64
		Context []byte
	}
	RaftProposeRequest struct {
		Op         RaftOp
		Data       []byte
		WithResult bool

		reqId   string
		respKey string
	}
	RaftProposeResponse struct {
		Data interface{}

		respKey string
	}

	RaftManager struct{}
	RaftGroup   struct {
		applyIndex       uint64
		stableApplyIndex uint64
		sm               RaftStateMachine
		storage          RaftStorage
		raft             Raft
		proposeResponses sync.Map
	}
	RaftNoop struct{}
)

func (r *RaftNoop) Propose(ctx context.Context, data []byte) error {
	return nil
}

func (req *RaftProposeRequest) Marshal() ([]byte, error) {
	return nil, nil
}

func (req *RaftProposeRequest) Unmarshal(raw []byte) error {
	return nil
}

func (resp *RaftProposeResponse) Close() {
}

func (r *RaftGroup) Propose(ctx context.Context, req *RaftProposeRequest) (*RaftProposeResponse, error) {
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
	return &RaftProposeResponse{
		Data:    r.proposeResponses.Load(req.respKey),
		respKey: req.respKey,
	}, nil
}

func (r *RaftGroup) Start() {
}

func (r *RaftGroup) Apply(ctx context.Context, data []byte, index uint64) error {
	req := &RaftProposeRequest{}
	if err := req.Unmarshal(data); err != nil {
		return err
	}
	_, applyCtx := trace.StartSpanFromContextWithTraceID(ctx, "", req.reqId)
	result, err := r.sm.Apply(applyCtx, req.Op, req.Data, index)
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

func (r *RaftGroup) ApplyMemberChange(cc RaftConfChange, index uint64) error {
	return r.sm.ApplyMemberChange(cc, index)
}

func (r *RaftGroup) Snapshot() (RaftSnapshot, error) {
	return r.sm.Snapshot()
}

func (r *RaftGroup) ApplySnapshot(st RaftSnapshot) error {
	return r.sm.ApplySnapshot(st)
}

func (r *RaftGroup) LeaderChange(leader uint64, addr string) error {
	return r.sm.LeaderChange(leader, addr)
}
