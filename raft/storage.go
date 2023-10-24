package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	groupPrefix    = []byte("g")
	logIndexInfix  = []byte("i")
	confStateInfix = []byte("c")
	hardStateInfix = []byte("h")
)

type storageConfig struct {
	id              uint64
	maxSnapshotNum  int
	snapshotTimeout time.Duration
	members         []Member
	raw             Storage
	sm              StateMachine
}

func newStorage(cfg storageConfig) (*storage, error) {
	value, err := cfg.raw.Get(encodeHardStateKey(cfg.id))
	if err != nil && err != kvstore.ErrNotFound {
		return nil, err
	}
	defer value.Close()

	hs := &raftpb.HardState{}
	if err := hs.Unmarshal(value.Data()); err != nil {
		return nil, err
	}

	storage := &storage{
		id:               cfg.id,
		hardState:        *hs,
		rawStg:           cfg.raw,
		stateMachine:     cfg.sm,
		snapshotRecorder: newSnapshotRecorder(cfg.maxSnapshotNum, cfg.snapshotTimeout),
	}
	members := make(map[uint64]Member)
	for i := range cfg.members {
		members[cfg.members[i].NodeID] = cfg.members[i]
	}
	storage.membersMu.members = members
	storage.updateConfState()

	return storage, nil
}

type storage struct {
	id           uint64
	firstIndex   uint64
	lastIndex    uint64
	appliedIndex uint64
	hardState    raftpb.HardState
	membersMu    struct {
		sync.RWMutex
		members map[uint64]Member
		cs      raftpb.ConfState
	}

	rawStg           Storage
	stateMachine     StateMachine
	snapshotRecorder *snapshotRecorder
}

// InitialState returns the saved HardState and ConfState information.
func (s *storage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	return s.hardState, s.membersMu.cs, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *storage) Entries(lo, hi, maxSize uint64) (ret []raftpb.Entry, err error) {
	iter := s.rawStg.Iter(encodeIndexLogKey(s.id, lo))
	defer iter.Close()

	for {
		if !iter.Next() {
			break
		}
		if iter.Err() != nil {
			return nil, iter.Err()
		}

		entry := &raftpb.Entry{}
		if err := entry.Unmarshal(iter.Value().Data()); err != nil {
			return nil, err
		}
		ret = append(ret, *entry)

		if uint64(len(ret)) == maxSize {
			return
		}
	}

	return
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *storage) Term(i uint64) (uint64, error) {
	value, err := s.rawStg.Get(encodeIndexLogKey(s.id, i))
	if err != nil {
		return 0, err
	}

	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(value.Data()); err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (s *storage) LastIndex() (uint64, error) {
	if s.lastIndex > 0 {
		return s.lastIndex, nil
	}

	iterator := s.rawStg.Iter(nil)
	defer iterator.Close()

	if err := iterator.SeekForPrev(encodeIndexLogKey(s.id, math.MaxUint64)); err != nil {
		return 0, err
	}
	if !iterator.Next() {
		return 0, nil
	}
	if iterator.Err() != nil {
		return 0, iterator.Err()
	}

	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(iterator.Value().Data()); err != nil {
		return 0, err
	}

	s.lastIndex = entry.Index
	return s.lastIndex, nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (s *storage) FirstIndex() (uint64, error) {
	if s.firstIndex > 0 {
		return s.firstIndex, nil
	}

	iterator := s.rawStg.Iter(encodeIndexLogKey(s.id, 0))
	defer iterator.Close()

	if !iterator.Next() {
		return 0, nil
	}
	if iterator.Err() != nil {
		return 0, iterator.Err()
	}

	entry := &raftpb.Entry{}
	if err := entry.Unmarshal(iterator.Value().Data()); err != nil {
		return 0, err
	}

	s.firstIndex = entry.Index
	return s.firstIndex, nil
}

// Snapshot returns the most recent outgoingSnapshot.
// If outgoingSnapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// outgoingSnapshot and call Snapshot later.
func (s *storage) Snapshot() (raftpb.Snapshot, error) {
	var members []Member
	s.membersMu.RLock()
	for _, member := range s.membersMu.members {
		members = append(members, member)
	}
	cs := s.membersMu.cs
	s.membersMu.RUnlock()

	smSnap := s.stateMachine.Snapshot()
	success := false
	defer func() {
		if !success {
			smSnap.Close()
		}
	}()
	appliedIndex := s.AppliedIndex()

	smSnapIndex := smSnap.Index()
	if smSnapIndex > appliedIndex {
		return raftpb.Snapshot{}, fmt.Errorf("state machine outgoingSnapshot index[%d] greater than applied index[%d]", smSnapIndex, appliedIndex)
	}

	term, err := s.Term(smSnapIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	snapshot := newOutgoingSnapshot(&RaftSnapshotHeader{
		ID:                 uuid.New().String(),
		RaftMessageRequest: nil,
	}, smSnap)
	s.snapshotRecorder.Set(snapshot)
	success = true

	return raftpb.Snapshot{
		Data: []byte(snapshot.ID),
		Metadata: raftpb.SnapshotMetadata{
			ConfState: cs,
			Index:     appliedIndex,
			Term:      term,
		},
	}, nil
}

func (s *storage) AppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *storage) SetAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.appliedIndex, index)
}

// SaveHardStateAndEntries is called by one worker only
func (s *storage) SaveHardStateAndEntries(hs raftpb.HardState, entries []raftpb.Entry) error {
	batch := s.rawStg.NewBatch()

	value, err := hs.Marshal()
	if err != nil {
		return err
	}
	batch.Put(encodeHardStateKey(s.id), value)

	lastIndex := uint64(0)
	for i := range entries {
		key := encodeIndexLogKey(s.id, entries[i].Index)
		value, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		batch.Put(key, value)
		lastIndex = entries[i].Index
	}
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	// update last index after save new entries
	if lastIndex > 0 {
		atomic.StoreUint64(&s.lastIndex, lastIndex)
	}

	s.hardState = hs
	return nil
}

// Truncate may be called by top level application concurrently
func (s *storage) Truncate(ctx context.Context, index uint64) error {
	// todo: can not truncate log index which large than the alive outgoingSnapshot's index

	batch := s.rawStg.NewBatch()
	batch.DeleteRange(encodeIndexLogKey(s.id, 0), encodeIndexLogKey(s.id, index))
	if err := s.rawStg.Write(batch); err != nil {
		return err
	}

	// update first index after truncate wal log
	for {
		firstIndex := atomic.LoadUint64(&s.firstIndex)
		if firstIndex > index {
			return nil
		}
		if atomic.CompareAndSwapUint64(&s.firstIndex, firstIndex, index) {
			return nil
		}
	}
}

func (s *storage) GetSnapshot(id string) *outgoingSnapshot {
	return s.snapshotRecorder.Get(id)
}

func (s *storage) DeleteSnapshot(id string) {
	s.snapshotRecorder.Delete(id)
}

func (s *storage) NewBatch() Batch {
	return s.rawStg.NewBatch()
}

func (s *storage) MemberChange(member *Member) {
	s.membersMu.Lock()
	s.membersMu.members[member.NodeID] = *member
	s.membersMu.Unlock()
}

func (s *storage) updateConfState() {
	s.membersMu.Lock()
	for _, m := range s.membersMu.members {
		if m.Learner {
			s.membersMu.cs.Learners = append(s.membersMu.cs.Learners, m.NodeID)
		} else {
			s.membersMu.cs.Voters = append(s.membersMu.cs.Voters, m.NodeID)
		}
	}
	s.membersMu.Unlock()
	return
}

func encodeIndexLogKey(id uint64, index uint64) []byte {
	b := make([]byte, 8+8+len(groupPrefix)+len(logIndexInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], logIndexInfix)
	binary.BigEndian.PutUint64(b[8+len(groupPrefix)+len(logIndexInfix):], index)

	return b
}

func encodeHardStateKey(id uint64) []byte {
	b := make([]byte, 8+len(groupPrefix)+len(hardStateInfix))
	copy(b, groupPrefix)
	binary.BigEndian.PutUint64(b[len(groupPrefix):], id)
	copy(b[8+len(groupPrefix):], hardStateInfix)

	return b
}
