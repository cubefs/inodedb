package raft

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	stateQueued uint8 = 1 << iota
	stateProcessTick
	stateProcessRaftRequestMsg
	stateProcessReady
)

const (
	defaultGroupStateMuShardCount = 32
	defaultTickIntervalMS         = 200
	defaultHeartbeatTickInterval  = 10
	defaultElectionTickInterval   = 10 * defaultHeartbeatTickInterval
	defaultWorkerNum              = 96
	defaultSnapshotWorkerNum      = 16
	defaultSnapshotTimeoutS       = 3600
	defaultSizePerMsg             = 1 << 20
	defaultInflightMsg            = 128
	defaultProposeMsgNum          = 256
	defaultSnapshotNum            = 3
	defaultProposeTimeoutMS       = 5000
	defaultReadIndexTimeoutMS     = defaultProposeTimeoutMS
)

type (
	Manager interface {
		CreateRaftGroup(ctx context.Context, cfg *GroupConfig) error
		GetRaftGroup(id uint64) (Group, error)
	}

	groupHandler interface {
		HandlePropose(ctx context.Context, id uint64, req proposalRequest) error
		HandleSnapshot(ctx context.Context, id uint64, message raftpb.Message) error
		HandleSendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest, class connectionClass) error
		HandleSendRaftSnapshotRequest(ctx context.Context, snapshot *outgoingSnapshot) error
		HandleSignalToWorker(ctx context.Context, id uint64)
		HandleMaybeCoalesceHeartbeat(ctx context.Context, groupId uint64, msg *raftpb.Message) bool
		HandleNextID() uint64
	}

	groupProcessor interface {
		ID() uint64
		NodeID() uint64
		Tick()
		WithRaftRawNodeLocked(f func(rn *raft.RawNode) error) error
		ProcessSendRaftMessage(ctx context.Context, messages []raftpb.Message)
		ProcessSendSnapshot(ctx context.Context, m raftpb.Message)
		ProcessRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) error
		ProcessRaftSnapshotRequest(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error
		SaveHardStateAndEntries(ctx context.Context, hs raftpb.HardState, entries []raftpb.Entry) error
		ApplyLeaderChange(nodeID uint64) error
		ApplyCommittedEntries(ctx context.Context, entries []raftpb.Entry) (err error)
		ApplyReadIndex(ctx context.Context, readState raft.ReadState)
		AddUnreachableRemoteReplica(remote uint64)
	}
)

type (
	Config struct {
		NodeID uint64
		// TickIntervalMs is the millisecond interval of timer which check heartbeat and election timeout.
		// The default value is 200ms.
		TickIntervalMS int
		// HeartbeatTick is the heartbeat interval. A leader sends heartbeat
		// message to maintain the leadership every heartbeat interval.
		// The default value is 10x of TickInterval.
		HeartbeatTick int
		// ElectionTick is the election timeout. If a follower does not receive any message
		// from the leader of current term during ElectionTick, it will become candidate and start an election.
		// ElectionTick must be greater than HeartbeatTick.
		// We suggest to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary leader switching.
		// The default value is 10x of HeartbeatTick.
		ElectionTick int
		// CoalescedHeartbeatsInterval specifies the coalesced heartbeat intervals
		// The default value is the half of TickInterval.
		CoalescedHeartbeatsInterval int
		// MaxSizePerMsg limits the max size of each append message.
		// The default value is 1M.
		MaxSizePerMsg uint64
		// MaxInflightMsg limits the max number of in-flight append messages during optimistic replication phase.
		// The application transportation layer usually has its own sending buffer over TCP/UDP.
		// Setting MaxInflightMsgs to avoid overflowing that sending buffer.
		// The default value is 128.
		MaxInflightMsg int
		// ReadOnlyOption specifies how the read only request is processed.
		//
		// ReadOnlySafe guarantees the linearizability of the read only request by
		// communicating with the quorum. It is the default and suggested option.
		//
		// ReadOnlyLeaseBased ensures linearizability of the read only request by
		// relying on the leader lease. It can be affected by clock drift.
		// If the clock drift is unbounded, leader might keep the lease longer than it
		// should (clock can move backward/pause without any bound). ReadIndex is not safe
		// in that case.
		// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
		ReadOnlyOption raft.ReadOnlyOption
		// MaxProposeMsgNum specifies the max raft step msg num
		// The default value is 256.
		MaxProposeMsgNum int
		// MaxWorkerNum specifies the max worker num of raft group processing
		// The default value is 96.
		MaxWorkerNum int
		// MaxSnapshotWorkerNum specifies the max snapshot worker num of sending raft snapshot
		// The default value is 16.
		MaxSnapshotWorkerNum int
		// MaxSnapshotNum limits the max number of snapshot num per raft group.
		// The default value is 3.
		MaxSnapshotNum int
		// SnapshotTimeoutS limits the max expire time of snapshot
		// The default value is 1 hour.
		SnapshotTimeoutS int
		// ReadOption specifies the linearizability  of the read option
		ReadOption raft.ReadOnlyOption
		// ProposeTimeout specifies the proposal timeout interval
		// The default value is 5s.
		ProposeTimeoutMS int
		// ReadIndexTimeout specifies the read index timeout interval
		// The default value is 5s.
		ReadIndexTimeoutMS int

		TransportConfig TransportConfig
		Logger          raft.Logger
		Storage         Storage
		Resolver        AddressResolver
	}
	GroupConfig struct {
		ID      uint64
		Term    uint64
		Applied uint64
		Members []Member
		SM      StateMachine
	}

	groupState struct {
		state uint8
		id    uint64
	}
	snapshotMessage struct {
		groupID uint64
		message raftpb.Message
	}
)

func NewManager(cfg *Config) (Manager, error) {
	initConfig(cfg)
	if cfg.Logger != nil {
		raft.SetLogger(cfg.Logger)
	}

	manager := &manager{
		idGenerator:   newIDGenerator(cfg.NodeID, time.Now()),
		snapshotQueue: make(chan snapshotMessage, 1024),
		cfg:           cfg,
		done:          make(chan struct{}),
	}

	cfg.TransportConfig.Resolver = &cacheAddressResolver{resolver: cfg.Resolver}
	cfg.TransportConfig.Handler = (*internalTransportHandler)()

	manager.transport = newTransport(&cfg.TransportConfig)
	for i := 0; i < cfg.MaxWorkerNum; i++ {
		workerCh := make(chan groupState)
		manager.workerChs = append(manager.workerChs, workerCh)
		go (*internalGroupHandler)(manager).worker(workerCh)
	}
	for i := 0; i < cfg.MaxSnapshotWorkerNum; i++ {
		go (*internalGroupHandler)(manager).snapshotWorker()
	}

	return manager, nil
}

type manager struct {
	groups            sync.Map
	proposalQueues    sync.Map
	raftMessageQueues sync.Map
	snapshotQueue     chan snapshotMessage
	workerChs         []chan groupState
	coalescedMu       struct {
		sync.Mutex
		heartbeats         map[uint64][]RaftHeartbeat
		heartbeatResponses map[uint64][]RaftHeartbeat
	}
	groupStateMu [defaultGroupStateMuShardCount]struct {
		sync.RWMutex
		state map[uint64]groupState
	}
	workerRoundRobinCount uint32
	done                  chan struct{}

	idGenerator *idGenerator
	transport   *transport
	cfg         *Config
}

func (m *manager) CreateRaftGroup(ctx context.Context, cfg *GroupConfig) error {
	storage, err := newStorage(storageConfig{
		id:              cfg.ID,
		maxSnapshotNum:  m.cfg.MaxSnapshotNum,
		snapshotTimeout: time.Duration(m.cfg.SnapshotTimeoutS) * time.Second,
		members:         cfg.Members,
		raw:             m.cfg.Storage,
		sm:              cfg.SM,
	})
	if err != nil {
		return errors.Info(err, "mew raft storage failed")
	}

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              m.cfg.NodeID,
		ElectionTick:    m.cfg.ElectionTick,
		HeartbeatTick:   m.cfg.HeartbeatTick,
		Storage:         storage,
		Applied:         cfg.Applied,
		MaxSizePerMsg:   m.cfg.MaxSizePerMsg,
		MaxInflightMsgs: m.cfg.MaxInflightMsg,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          m.cfg.Logger,
	})
	if err != nil {
		return err
	}

	g := &group{
		id:           cfg.ID,
		nodeID:       m.cfg.NodeID,
		appliedIndex: cfg.Applied,

		sm:      cfg.SM,
		storage: storage,
		handler: (*internalGroupHandler)(m),
	}
	g.rawNodeMu.rawNode = rawNode

	_, loaded := m.groups.LoadOrStore(cfg.ID, g)
	if loaded {
		return errors.New("group already exists")
	}

	queue := newProposalQueue(m.cfg.MaxProposeMsgNum)
	m.proposalQueues.Store(cfg.ID, queue)
	return nil
}

func (m *manager) GetRaftGroup(id uint64) (Group, error) {
	v, ok := m.groups.Load(id)
	if !ok {
		return nil, errors.New("group not found")
	}
	return v.(Group), nil
}

func (m *manager) RemoveRaftGroup(id uint64) {
	m.groups.LoadAndDelete(id)
	m.proposalQueues.LoadAndDelete(id)
}

func (h *internalGroupHandler) processTick(ctx context.Context, g groupProcessor) {
	g.Tick()
}

func (h *internalGroupHandler) processRaftRequestMsg(ctx context.Context, g groupProcessor) bool {
	span := trace.SpanFromContext(ctx)
	value, ok := h.raftMessageQueues.Load(g.ID())
	if !ok {
		return false
	}

	q := value.(*raftMessageQueue)
	infos, ok := q.drain()
	if !ok {
		return false
	}
	defer q.recycle(infos)

	var hadError bool
	for i := range infos {
		info := &infos[i]
		if pErr := g.ProcessRaftMessageRequest(ctx, info.req); pErr != nil {
			hadError = true
			if err := info.respStream.Send(newRaftMessageResponse(info.req, newError(ErrCodeGroupHandleRaftMessage, pErr.Error()))); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				span.Errorf("req[%+v] sending response error: %s", info.req, err)
			}
		}
	}

	if hadError {
		// dropping the request queue to free up space when group has been deleted
		if _, exists := h.groups.Load(g.ID()); !exists {
			q.Lock()
			if len(q.infos) == 0 {
				h.raftMessageQueues.Delete(g.ID())
			}
			q.Unlock()
		}
	}

	return true
}

// process group ready
// 	1. get all proposal message and send request msg
//	2. apply soft state
//  3. send raft request messages
// 	4. save hard state and entries into wal log before apply committed entries,
//	   they should be packed into one write batch with hard state
// 	5. apply committed entries (include conf state and normal entry)
func (h *internalGroupHandler) processReady(ctx context.Context, g groupProcessor) {
	span := trace.SpanFromContext(ctx)

	if err := h.processProposal(g); err != nil {
		span.Fatalf("process proposal msg failed: %s", err)
	}

	var rd raft.Ready
	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		hasReady := rn.HasReady()
		if !hasReady {
			return nil
		}
		rd = rn.Ready()
		return nil
	})

	if rd.SoftState != nil {
		if err := g.ApplyLeaderChange(rd.SoftState.Lead); err != nil {
			span.Fatalf("leader change notify failed: %s", err)
		}
	}

	g.ProcessSendRaftMessage(ctx, rd.Messages)

	if err := g.SaveHardStateAndEntries(ctx, rd.HardState, rd.Entries); err != nil {
		span.Fatalf("save hard state and entries failed: %s", err)
	}
	if len(rd.CommittedEntries) > 0 {
		if err := g.ApplyCommittedEntries(ctx, rd.CommittedEntries); err != nil {
			span.Fatalf("apply committed entries failed: %s", err)
		}
	}
	for i := range rd.ReadStates {
		g.ApplyReadIndex(ctx, rd.ReadStates[i])
	}

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.Advance(rd)
		return nil
	})
}

func (h *internalGroupHandler) processProposal(g groupProcessor) (err error) {
	var data []byte
	v, ok := h.proposalQueues.Load(g.ID())
	if !ok {
		// todo: log
		return nil
	}
	queue := v.(*proposalQueue)

	// TODO: reuse with entries pool as it's capacity is fixed
	entries := make([]raftpb.Entry, 0, h.cfg.MaxProposeMsgNum)
	queue.Iter(func(p proposalRequest) bool {
		data, err = p.data.Marshal()
		if err != nil {
			return false
		}
		entries = append(entries, raftpb.Entry{
			Type: p.entryType,
			Data: data,
		})
		return true
	})

	return g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(raftpb.Message{
			Type:    raftpb.MsgProp,
			From:    g.NodeID(),
			Entries: entries,
		})
	})
}

func (m *manager) raftTickLoop() {
	ticker := time.NewTicker(time.Duration(m.cfg.TickIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	var groupIDs []uint64

	for {
		select {
		case <-ticker.C:
			groupIDs = groupIDs[:0]
			m.groups.Range(func(key, value interface{}) bool {
				groupIDs = append(groupIDs, key.(uint64))
				return true
			})

			for _, id := range groupIDs {
				(*internalGroupHandler)(m).signalToWorker(id, stateProcessTick)
			}
		case <-m.done:
			return
		}
	}
}

func (m *manager) coalescedHeartbeatsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.CoalescedHeartbeatsInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.sendQueuedHeartbeats(ctx)
		case <-m.done:
			return
		}
	}
}

func (m *manager) sendQueuedHeartbeats(ctx context.Context) {
	m.coalescedMu.Lock()
	heartbeats := m.coalescedMu.heartbeats
	heartbeatResponses := m.coalescedMu.heartbeatResponses
	m.coalescedMu.heartbeats = map[uint64][]RaftHeartbeat{}
	m.coalescedMu.heartbeatResponses = map[uint64][]RaftHeartbeat{}
	m.coalescedMu.Unlock()

	var beatsSent int

	for toNodeID, beats := range heartbeats {
		beatsSent += m.sendQueuedHeartbeatsToNode(ctx, beats, nil, toNodeID)
	}
	for toNodeID, resps := range heartbeatResponses {
		beatsSent += m.sendQueuedHeartbeatsToNode(ctx, nil, resps, toNodeID)
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (m *manager) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []RaftHeartbeat, toNodeID uint64,
) int {
	var (
		msgType raftpb.MessageType
		span    = trace.SpanFromContext(ctx)
	)

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	}
	if len(beats) > 0 && len(resps) > 0 {
		span.Fatalf("can't send heartbeat request and response both")
	}

	if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	}
	if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	}

	chReq := newRaftMessageRequest()
	*chReq = RaftMessageRequest{
		GroupID: 0,
		To:      toNodeID,
		From:    m.cfg.NodeID,
		Message: raftpb.Message{
			Type: msgType,
		},
		Heartbeats:         beats,
		HeartbeatResponses: resps,
	}

	if err := m.transport.SendAsync(ctx, chReq, systemConnectionClass); err != nil {
		for i := range beats {
			if value, ok := m.groups.Load(beats[i].GroupID); ok {
				value.(*internalGroupProcessor).AddUnreachableRemoteReplica(chReq.To)
			}
		}
		for i := range resps {
			if value, ok := m.groups.Load(resps[i].GroupID); ok {
				value.(*internalGroupProcessor).AddUnreachableRemoteReplica(chReq.To)
			}
		}

		span.Errorf("send queued heartbeats failed: %s", err)
		return 0
	}
	return len(beats) + len(resps)
}

type internalGroupHandler manager

func (h *internalGroupHandler) HandlePropose(ctx context.Context, id uint64, req proposalRequest) error {
	v, ok := h.proposalQueues.Load(id)
	if !ok {
		// todo: log
		return nil
	}
	queue := v.(proposalQueue)

	if err := queue.Push(ctx, req); err != nil {
		return err
	}

	h.signalToWorker(id, stateProcessReady)
	return nil
}

func (h *internalGroupHandler) HandleSnapshot(ctx context.Context, id uint64, message raftpb.Message) error {
	select {
	case h.snapshotQueue <- snapshotMessage{
		groupID: id,
		message: message,
	}:
		return nil
	default:
		return errors.New("snapshot queue full")
	}
}

func (h *internalGroupHandler) HandleMaybeCoalesceHeartbeat(ctx context.Context, groupId uint64, msg *raftpb.Message) bool {
	var hbMap map[uint64][]RaftHeartbeat

	switch msg.Type {
	case raftpb.MsgHeartbeat:
		h.coalescedMu.Lock()
		hbMap = h.coalescedMu.heartbeats
	case raftpb.MsgHeartbeatResp:
		h.coalescedMu.Lock()
		hbMap = h.coalescedMu.heartbeatResponses
	default:
		return false
	}
	hb := RaftHeartbeat{
		GroupID: groupId,
		To:      msg.To,
		From:    msg.From,
		Term:    msg.Term,
		Commit:  msg.Commit,
	}

	hbMap[msg.To] = append(hbMap[msg.To], hb)
	h.coalescedMu.Unlock()
	return true
}

func (h *internalGroupHandler) HandleSendRaftMessageRequest(ctx context.Context, req *RaftMessageRequest, class connectionClass) error {
	return h.transport.SendAsync(ctx, req, class)
}

func (h *internalGroupHandler) HandleSendRaftSnapshotRequest(ctx context.Context, snapshot *outgoingSnapshot) error {
	return h.transport.SendSnapshot(ctx, snapshot)
}

func (h *internalGroupHandler) HandleSignalToWorker(ctx context.Context, id uint64) {
	h.signalToWorker(id, stateProcessReady)
}

func (h *internalGroupHandler) HandleNextID() uint64 {
	return h.idGenerator.Next()
}

func (h *internalGroupHandler) signalToWorker(groupID uint64, state uint8) {
	if !h.enqueueGroupState(groupID, state) {
		return
	}

	count := atomic.AddUint32(&h.workerRoundRobinCount, 1)
	for {
		select {
		case h.workerChs[int(count)%len(h.workerChs)] <- groupState{state: state, id: groupID}:
			break
		default:
			count = count + uint32(rand.Int31n(int32(len(h.workerChs))))
		}
	}
}

// worker do raft state processing job, one raft group will be run in the worker pool with one worker only.
func (h *internalGroupHandler) worker(ch chan groupState) {
	for {
		select {
		case in := <-ch:
			_, ctx := trace.StartSpanFromContext(context.Background(), "")
			v, ok := h.groups.Load(in.id)
			if !ok {
				// TODO: log warn as group may be remove when raft group migrate happened
				continue
			}
			group := v.(groupProcessor)

		AGAIN:

			// reset group state into queued, avaid raft group processing currently in worker pool
			h.setGroupStateForce(group.ID(), stateQueued)

			// process group request msg
			if in.state&stateProcessRaftRequestMsg != 0 {
				if h.processRaftRequestMsg(ctx, group) {
					in.state |= stateProcessReady
				}
			}

			// process group tick
			if in.state&stateProcessTick != 0 {
				h.processTick(ctx, group)
				in.state |= stateProcessReady
			}

			// process group ready
			if in.state&stateProcessReady != 0 {
				h.processReady(ctx, group)
			}

			state := h.getGroupState(group.ID())
			if state == stateQueued {
				continue
			}
			// new state signal coming, we do the group processing job again in this worker
			in.state = state
			goto AGAIN

		case <-h.done:
			return
		}
	}
}

func (h *internalGroupHandler) snapshotWorker() {
	for {
		select {
		case m := <-h.snapshotQueue:
			_, ctx := trace.StartSpanFromContext(context.Background(), "")
			v, ok := h.groups.Load(m.groupID)
			if !ok {
				// todo: add log when group has been removed
				continue
			}
			group := v.(groupProcessor)
			group.ProcessSendSnapshot(ctx, m.message)
		case <-h.done:
			return
		}
	}
}

func (h *internalGroupHandler) enqueueGroupState(groupID uint64, state uint8) (enqueued bool) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	prevState := stateMu.state[groupID]

	if prevState.state&state == state {
		return false
	}

	newState := prevState
	newState.state = newState.state | state
	if newState.state&stateQueued == 0 {
		newState.state |= stateQueued
		enqueued = true
	}
	stateMu.state[groupID] = newState

	return
}

func (h *internalGroupHandler) setGroupStateForce(groupID uint64, state uint8) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.Lock()
	defer stateMu.Unlock()

	stateMu.state[groupID] = groupState{
		state: state,
		id:    groupID,
	}
}

func (h *internalGroupHandler) getGroupState(groupID uint64) (state uint8) {
	muIndex := groupID % uint64(len(h.groupStateMu))
	stateMu := h.groupStateMu[muIndex]

	stateMu.RLock()
	defer stateMu.RUnlock()

	return stateMu.state[groupID].state
}

type internalTransportHandler manager

func (t *internalTransportHandler) HandleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream MessageResponseStream,
) error {
	span := trace.SpanFromContext(ctx)

	if len(req.Heartbeats)+len(req.HeartbeatResponses) > 0 {
		if req.GroupID != 0 {
			span.Fatalf("coalesced heartbeat's group id must be 0")
		}
		t.uncoalesceBeats(ctx, req.Heartbeats, raftpb.MsgHeartbeat, respStream)
		t.uncoalesceBeats(ctx, req.HeartbeatResponses, raftpb.MsgHeartbeatResp, respStream)
		return nil
	}
	enqueue := t.handleRaftUncoalescedRequest(ctx, req, respStream)
	if enqueue {
		(*internalGroupHandler)(t).signalToWorker(req.GroupID, stateProcessRaftRequestMsg)
	}

	return nil
}

func (t *internalTransportHandler) HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error {
	if resp.Err != nil {
		switch resp.Err {
		case ErrGroupHandlerNotFound:
		case ErrRaftGroupDeleted:
			// todo: remove raft group
		case ErrReplicaTooOld:
			// todo: remove replica
		}
	}
	return nil
}

func (t *internalTransportHandler) HandleRaftSnapshot(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error {
	raftMessage := req.Header.RaftMessageRequest
	v, ok := t.groups.Load(raftMessage.GroupID)
	if !ok {
		return stream.Send(&RaftSnapshotResponse{
			Status:  RaftSnapshotResponse_ERROR,
			Message: ErrGroupNotFound.Error,
		})
	}

	g := v.(groupProcessor)
	if err := g.ProcessRaftSnapshotRequest(ctx, req, stream); err != nil {
		return stream.Send(&RaftSnapshotResponse{
			Status:  RaftSnapshotResponse_ERROR,
			Message: err.Error(),
		})
	}

	if err := g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(raftMessage.Message)
	}); err != nil {
		return err
	}

	return stream.Send(&RaftSnapshotResponse{Status: RaftSnapshotResponse_APPLIED})
}

func (t *internalTransportHandler) uncoalesceBeats(
	ctx context.Context,
	beats []RaftHeartbeat,
	msgT raftpb.MessageType,
	respStream MessageResponseStream,
) {
	if len(beats) == 0 {
		return
	}

	beatReqs := make([]RaftMessageRequest, len(beats))
	var groupIDs []uint64
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   beat.From,
			To:     beat.To,
			Term:   beat.Term,
			Commit: beat.Commit,
		}
		beatReqs[i] = RaftMessageRequest{
			GroupID: beat.GroupID,
			From:    beat.From,
			To:      beat.To,
			Message: msg,
		}

		enqueue := t.handleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream)
		if enqueue {
			groupIDs = append(groupIDs, beat.GroupID)
		}
	}

	for _, id := range groupIDs {
		(*internalGroupHandler)(t).signalToWorker(id, stateProcessRaftRequestMsg)
	}
}

func (t *internalTransportHandler) handleRaftUncoalescedRequest(
	ctx context.Context, req *RaftMessageRequest, respStream MessageResponseStream,
) (enqueue bool) {
	span := trace.SpanFromContext(ctx)
	if len(req.Heartbeats)+len(req.HeartbeatResponses) > 0 {
		span.Fatalf("handleRaftUncoalescedRequest can not handle heartbeats and heartbeat responses, but received %+v", req)
	}

	value, ok := t.raftMessageQueues.Load(req.GroupID)
	if !ok {
		value, _ = t.raftMessageQueues.LoadOrStore(req.GroupID, unsafe.Pointer(&raftMessageQueue{}))
	}

	q := value.(*raftMessageQueue)
	q.Lock()
	defer q.Unlock()

	q.infos = append(q.infos, raftMessageInfo{
		req:        req,
		respStream: respStream,
	})

	return len(q.infos) == 1
}

type raftMessageInfo struct {
	req        *RaftMessageRequest
	respStream MessageResponseStream
}

type raftMessageQueue struct {
	infos []raftMessageInfo
	sync.Mutex
}

func (q *raftMessageQueue) drain() ([]raftMessageInfo, bool) {
	q.Lock()
	defer q.Unlock()

	if len(q.infos) == 0 {
		return nil, false
	}
	infos := q.infos
	q.infos = nil
	return infos, true
}

func (q *raftMessageQueue) recycle(processed []raftMessageInfo) {
	if cap(processed) > 32 {
		return
	}
	q.Lock()
	defer q.Unlock()

	if q.infos == nil {
		for i := range processed {
			processed[i] = raftMessageInfo{}
		}
		q.infos = processed[:0]
	}
}

var raftMessageRequestPool = sync.Pool{
	New: func() interface{} {
		return &RaftMessageRequest{}
	},
}

func newRaftMessageRequest() *RaftMessageRequest {
	return raftMessageRequestPool.Get().(*RaftMessageRequest)
}

func (m *RaftMessageRequest) release() {
	*m = RaftMessageRequest{}
	raftMessageRequestPool.Put(m)
}

func initConfig(cfg *Config) {
	initialDefaultConfig(&cfg.TickIntervalMS, defaultTickIntervalMS)
	initialDefaultConfig(&cfg.HeartbeatTick, defaultHeartbeatTickInterval)
	initialDefaultConfig(&cfg.ElectionTick, defaultElectionTickInterval)
	initialDefaultConfig(&cfg.CoalescedHeartbeatsInterval, cfg.TickIntervalMS/2)
	initialDefaultConfig(&cfg.MaxWorkerNum, defaultWorkerNum)
	initialDefaultConfig(&cfg.MaxSnapshotWorkerNum, defaultSnapshotWorkerNum)
	initialDefaultConfig(&cfg.SnapshotTimeoutS, defaultSnapshotTimeoutS)
	initialDefaultConfig(&cfg.MaxInflightMsg, defaultInflightMsg)
	initialDefaultConfig(&cfg.MaxSnapshotNum, defaultSnapshotNum)
	initialDefaultConfig(&cfg.MaxSizePerMsg, defaultSizePerMsg)
	initialDefaultConfig(&cfg.ProposeTimeoutMS, defaultProposeTimeoutMS)
	initialDefaultConfig(&cfg.ReadIndexTimeoutMS, defaultReadIndexTimeoutMS)
	initialDefaultConfig(&cfg.MaxProposeMsgNum, defaultProposeMsgNum)
}

func initialDefaultConfig(t interface{}, defaultValue interface{}) {
	switch t.(type) {
	case *int:
		*(t.(*int)) = defaultValue.(int)
	default:
	}
}
