package raft

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
)

// outgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a outgoingSnapshot over the network.
type outgoingSnapshotStream interface {
	Send(request *RaftSnapshotRequest) error
	Recv() (*RaftSnapshotResponse, error)
}

func newOutgoingSnapshot(header *RaftSnapshotHeader, st Snapshot) *outgoingSnapshot {
	return &outgoingSnapshot{
		RaftSnapshotHeader: header,
		st:                 st,
	}
}

// RaftSnapshotHeader
type outgoingSnapshot struct {
	*RaftSnapshotHeader

	st     Snapshot
	expire time.Time
}

// todo: limit the snapshot transmitting speed
func (s *outgoingSnapshot) Send(ctx context.Context, stream outgoingSnapshotStream) error {
	// send header firstly
	req := &RaftSnapshotRequest{Header: s.RaftSnapshotHeader}
	if err := stream.Send(req); err != nil {
		return err
	}
	req.Header = nil

	for !req.Final {
		batch, err := s.st.ReadBatch()
		if err != nil {
			if err != io.EOF {
				return err
			}
			req.Final = true
		}

		if batch != nil {
			req.Data = batch.Data()
		}
		if err := stream.Send(req); err != nil {
			if batch != nil {
				batch.Close()
			}
			return err
		}

		batch.Close()
		req.Data = nil
		req.Seq++
	}

	return nil
}

func (s *outgoingSnapshot) Close() {
	s.st.Close()
}

func newIncomingSnapshot(header *RaftSnapshotHeader, stream SnapshotResponseStream) *incomingSnapshot {
	return &incomingSnapshot{
		RaftSnapshotHeader: header,
		stream:             stream,
	}
}

// incomingSnapshot held the incoming snapshot and implements the Snapshot interface
// it will be used for state machine apply snapshot
type incomingSnapshot struct {
	*RaftSnapshotHeader

	final   bool
	seq     uint32
	storage *storage
	stream  SnapshotResponseStream
}

// todo: limit the snapshot transmitting speed
func (i *incomingSnapshot) ReadBatch() (Batch, error) {
	if i.final {
		return nil, io.EOF
	}

	req, err := i.stream.Recv()
	if err != nil {
		return nil, err
	}
	if req.Seq != i.seq {
		return nil, fmt.Errorf("unexpected snapshot request sequence: %d, expected: %d", req.Seq, i.seq)
	}

	var batch Batch
	if len(req.Data) > 0 {
		batch = i.storage.NewBatch()
		batch.From(req.Data)
	}

	i.final = req.Final
	i.seq++

	return batch, nil
}

func (i *incomingSnapshot) Index() uint64 {
	message := i.RaftMessageRequest.Message
	return message.Index
}

func (i *incomingSnapshot) Term() uint64 {
	message := i.RaftMessageRequest.Message
	return message.Term
}

func (i *incomingSnapshot) Close() error {
	return nil
}

func newSnapshotRecorder(maxSnapshot int, timeout time.Duration) *snapshotRecorder {
	sr := &snapshotRecorder{
		maxSnapshot: maxSnapshot,
		timeout:     timeout,
		evictList:   list.New(),
		snaps:       make(map[string]*list.Element),
	}

	return sr
}

type snapshotRecorder struct {
	sync.Mutex

	maxSnapshot int
	timeout     time.Duration
	evictList   *list.List
	snaps       map[string]*list.Element
}

func (s *snapshotRecorder) Set(st *outgoingSnapshot) error {
	s.Lock()
	defer s.Unlock()

	if s.evictList.Len() >= s.maxSnapshot {
		elem := s.evictList.Front()
		snap := elem.Value.(*outgoingSnapshot)
		if time.Since(snap.expire) < 0 {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		s.evictList.Remove(elem)
		elem.Value.(*outgoingSnapshot).Close()
		delete(s.snaps, snap.ID)
	}
	if _, hit := s.snaps[st.ID]; hit {
		return fmt.Errorf("outgoingSnapshot(%s) exist", st.ID)
	}
	st.expire = time.Now().Add(s.timeout)
	s.snaps[st.ID] = s.evictList.PushBack(st)
	return nil
}

func (s *snapshotRecorder) Get(key string) *outgoingSnapshot {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		snap := v.Value.(*outgoingSnapshot)
		snap.expire = time.Now().Add(s.timeout)
		s.evictList.MoveToBack(v)
		return snap
	}
	return nil
}

func (s *snapshotRecorder) Delete(key string) {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		delete(s.snaps, key)
		v.Value.(*outgoingSnapshot).Close()
		s.evictList.Remove(v)
	}
}

func (s *snapshotRecorder) Close() {
	s.Lock()
	defer s.Unlock()

	for key, val := range s.snaps {
		delete(s.snaps, key)
		val.Value.(*outgoingSnapshot).Close()
		s.evictList.Remove(val)
	}
}
