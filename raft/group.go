package raft

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Group interface {
	Propose(ctx context.Context, msg *ProposalData) (ProposalResponse, error)
	LeaderTransfer(ctx context.Context, peerID uint64) error
	ReadIndex(ctx context.Context) error
	Truncate(ctx context.Context, index uint64) error
	MemberChange(ctx context.Context, mc *Member) error
	Stat() (*Stat, error)
	Close() error
}

type group struct {
	id            uint64
	nodeID        uint64
	appliedIndex  uint64
	unreachableMu struct {
		sync.Mutex
		remotes map[uint64]struct{}
	}
	rawNodeMu struct {
		sync.Mutex
		rawNode *raft.RawNode
	}
	notifies sync.Map

	sm      StateMachine
	handler groupHandler
	storage *storage
}

func (g *group) Propose(ctx context.Context, data *ProposalData) (resp ProposalResponse, err error) {
	// if data.WithResult {
	data.notifyID = g.handler.HandleNextID()
	//}

	err = g.handler.HandlePropose(ctx, g.id, proposalRequest{
		data: data,
	})
	if err != nil {
		return
	}

	n := newNotify()
	g.addNotify(data.notifyID, n)

	ret, err := n.Wait(ctx)
	if err != nil {
		return
	}
	if ret.err != nil {
		return resp, ret.err
	}

	return ProposalResponse{Data: ret.reply}, nil
}

func (g *group) LeaderTransfer(ctx context.Context, peerID uint64) error {
	(*internalGroupProcessor)(g).WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.TransferLeader(peerID)
		return nil
	})
	g.handler.HandleSignalToWorker(ctx, g.id)
}

func (g *group) ReadIndex(ctx context.Context) error {
	notifyID := g.handler.HandleNextID()
	n := newNotify()
	g.addNotify(notifyID, n)

	(*internalGroupProcessor)(g).WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ReadIndex(notifyIDToBytes(notifyID))
		return nil
	})
	g.handler.HandleSignalToWorker(ctx, g.id)

	ret, err := n.Wait(ctx)
	if err != nil {
		return err
	}
	if ret.err != nil {
		return ret.err
	}
	return nil
}

func (g *group) Truncate(ctx context.Context, index uint64) error {
	return g.storage.Truncate(ctx, index)
}

func (g *group) MemberChange(ctx context.Context, mc *Member) error {
	data, err := mc.Marshal()
	if err != nil {
		return err
	}

	proposeData := &ProposalData{
		Data:     data,
		notifyID: g.handler.HandleNextID(),
	}

	if err = g.handler.HandlePropose(ctx, g.id, proposalRequest{
		entryType: raftpb.EntryConfChange,
		data:      proposeData,
	}); err != nil {
		return err
	}

	n := newNotify()
	g.addNotify(proposeData.notifyID, n)

	ret, err := n.Wait(ctx)
	if err != nil {
		return err
	}
	if ret.err != nil {
		return ret.err
	}

	return nil
}

func (g *group) Stat() (*Stat, error) {
	return nil, nil
}

func (g *group) Close() error {
	return nil
}

func (g *group) addNotify(notifyID uint64, n notify) {
	g.notifies.Store(notifyID, n)
}

func (g *group) doNotify(notifyID uint64, ret proposalResult) {
	n, ok := g.notifies.LoadAndDelete(notifyID)
	if !ok {
		return
	}
	n.(notify).Notify(ret)
}

type internalGroupProcessor group

func (g *internalGroupProcessor) ID() uint64 {
	return g.id
}

func (g *internalGroupProcessor) NodeID() uint64 {
	return g.nodeID
}

func (g *internalGroupProcessor) WithRaftRawNodeLocked(f func(rn *raft.RawNode) error) error {
	g.rawNodeMu.Lock()
	defer g.rawNodeMu.Unlock()

	return f(g.rawNodeMu.rawNode)
}

func (g *internalGroupProcessor) ProcessSendRaftMessage(ctx context.Context, messages []raftpb.Message) {
	span := trace.SpanFromContext(ctx)
	for i := range messages {
		msg := &messages[i]

		// add into async queue to process snapshot
		if msg.Type == raftpb.MsgSnap {
			if err := g.handler.HandleSnapshot(ctx, g.id, *msg); err != nil {
				// delete snapshot and report failed when handle snapshot into queue failed
				g.storage.DeleteSnapshot(string(msg.Snapshot.Data))
				g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
					rn.ReportSnapshot(msg.To, raft.SnapshotFailure)
					return nil
				})
			}
		}

		if g.handler.HandleMaybeCoalesceHeartbeat(ctx, g.id, msg) {
			continue
		}

		req := newRaftMessageRequest()
		*req = RaftMessageRequest{
			GroupID: g.id,
			To:      msg.To,
			From:    msg.From,
			Message: *msg,
		}
		if err := g.handler.HandleSendRaftMessageRequest(ctx, req, defaultConnectionClass); err != nil {
			g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
				rn.ReportUnreachable(msg.To)
				return nil
			})
			span.Warnf("handle send raft message request failed: %s", err)
			continue
		}
	}
}

func (g *internalGroupProcessor) ProcessSendSnapshot(ctx context.Context, m raftpb.Message) {
	span := trace.SpanFromContext(ctx)
	id := string(m.Snapshot.Data)
	snapshot := g.storage.GetSnapshot(id)
	if snapshot == nil {
		span.Errorf("not found outgoingSnapshot(%s)", id)
		g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
			rn.ReportSnapshot(m.To, raft.SnapshotFailure)
			return nil
		})
		return
	}
	defer g.storage.DeleteSnapshot(id)

	if err := g.handler.HandleSendRaftSnapshotRequest(ctx, snapshot); err != nil {
		g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
			span.Errorf("handle send raft outgoingSnapshot[%s] request failed: %s", id, err)
			rn.ReportSnapshot(m.To, raft.SnapshotFailure)
			return nil
		})
		return
	}

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ReportSnapshot(m.To, raft.SnapshotFinish)
		return nil
	})
}

func (g *internalGroupProcessor) ProcessRaftMessageRequest(ctx context.Context, req *RaftMessageRequest) error {
	span := trace.SpanFromContext(ctx)

	if req.Message.Type == raftpb.MsgSnap {
		span.Fatalf("receive unexpected outgoingSnapshot request: %+v", req)
	}

	if req.To == 0 {
		return errors.New("to node id can't be 0")
	}

	// drop := maybeDropMsgApp(ctx, (*replicaMsgAppDropper)(r), &req.Message, req.RangeStartKey)
	return g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		return rn.Step(req.Message)
	})
}

func (g *internalGroupProcessor) ProcessRaftSnapshotRequest(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error {
	snapshot := newIncomingSnapshot(req.Header, stream)
	return g.sm.ApplySnapshot(snapshot)
}

func (g *internalGroupProcessor) SaveHardStateAndEntries(ctx context.Context, hs raftpb.HardState, entries []raftpb.Entry) error {
	return g.storage.SaveHardStateAndEntries(hs, entries)
}

func (g *internalGroupProcessor) ApplyLeaderChange(nodeID uint64) error {
	return g.sm.LeaderChange(nodeID)
}

func (g *internalGroupProcessor) ApplyCommittedEntries(ctx context.Context, entries []raftpb.Entry) (err error) {
	allProposalData := make([]ProposalData, 0, len(entries))
	latestIndex := uint64(0)

	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryConfChange:
			// apply the previous committed entries first before apply conf change
			if len(allProposalData) > 0 {
				rets, err := g.sm.Apply(ctx, allProposalData, latestIndex)
				if err != nil {
					return errors.Info(err, "apply to state machine failed")
				}
				for j, ret := range rets {
					(*group)(g).doNotify(allProposalData[j].notifyID, proposalResult{
						reply: ret,
						err:   nil,
					})
				}
				allProposalData = allProposalData[0:0]
			}
			if err := g.applyConfChange(ctx, entries[i]); err != nil {
				return errors.Info(err, "apply conf change to state machine failed")
			}
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				continue
			}
			allProposalData = append(allProposalData, ProposalData{})
			proposalData := &allProposalData[i]
			if err = proposalData.Unmarshal(entries[i].Data); err != nil {
				return errors.Info(err, "unmarshal proposal data failed")
			}
		}

		latestIndex = entries[i].Index
	}

	if len(allProposalData) > 0 {
		rets, err := g.sm.Apply(ctx, allProposalData, latestIndex)
		if err != nil {
			return errors.Info(err, "apply to state machine failed")
		}

		for i, ret := range rets {
			(*group)(g).doNotify(allProposalData[i].notifyID, proposalResult{
				reply: ret,
				err:   nil,
			})
		}
	}

	g.storage.SetAppliedIndex(latestIndex)

	return
}

func (g *internalGroupProcessor) ApplyReadIndex(ctx context.Context, readState raft.ReadState) {
	notifyID := BytesToNotifyID(readState.RequestCtx)
	(*group)(g).doNotify(notifyID, proposalResult{})
}

func (g *internalGroupProcessor) Tick() {
	g.unreachableMu.Lock()
	remotes := g.unreachableMu.remotes
	g.unreachableMu.remotes = nil
	g.unreachableMu.Unlock()

	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		for remote := range remotes {
			rn.ReportUnreachable(remote)
		}
		rn.Tick()
		return nil
	})
}

func (g *internalGroupProcessor) AddUnreachableRemoteReplica(remote uint64) {
	g.unreachableMu.Lock()
	if g.unreachableMu.remotes == nil {
		g.unreachableMu.remotes = make(map[uint64]struct{})
	}
	g.unreachableMu.remotes[remote] = struct{}{}
	g.unreachableMu.Unlock()
}

func (g *internalGroupProcessor) applyConfChange(ctx context.Context, entry raftpb.Entry) error {
	var (
		cc   raftpb.ConfChange
		span = trace.SpanFromContext(ctx)
	)
	if err := cc.Unmarshal(entry.Data); err != nil {
		span.Fatalf("unmarshal conf change failed: %s", err)
		return err
	}

	// apply conf change to raft state machine
	g.WithRaftRawNodeLocked(func(rn *raft.RawNode) error {
		rn.ApplyConfChange(cc)
		return nil
	})

	member := &Member{}
	if err := member.Unmarshal(cc.Context); err != nil {
		return err
	}
	if err := g.sm.ApplyMemberChange(member, entry.Index); err != nil {
		return err
	}
	g.storage.MemberChange(member)
	return nil
}

func notifyIDToBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}

func BytesToNotifyID(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
