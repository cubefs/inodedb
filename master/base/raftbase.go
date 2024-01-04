package base

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/raft"
)

const (
	defaultTruncateNumInterval = uint64(50000)
	defaultPersistTimeInterval = 5
	LocalCF                    = "local_cf"
)

var (
	applyIndexKey = []byte("raft_apply_index")
	raftMemberKey = []byte("#raft_members")
)

type nodeManager struct {
	nodes map[uint64]*raft.Member
	nlk   sync.RWMutex
}

func (n *nodeManager) String() string {
	n.nlk.RLock()
	defer n.nlk.RUnlock()

	buf := bytes.NewBufferString("[")
	for i, m := range n.nodes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(m.String())
	}
	buf.WriteString("]")

	return buf.String()
}

// todo presist after update
func (n *nodeManager) addNode(nodeId uint64, m raft.Member) {
	n.nlk.Lock()
	defer n.nlk.Unlock()

	n.nodes[nodeId] = &m
}

func (n *nodeManager) removeNode(nodeId uint64) {
	n.nlk.Lock()
	defer n.nlk.Unlock()

	delete(n.nodes, nodeId)
}

func (n *nodeManager) getMembers() []raft.Member {
	n.nlk.RLock()
	defer n.nlk.RUnlock()

	mems := make([]raft.Member, 0)
	for _, m := range n.nodes {
		mems = append(mems, *m)
	}
	return mems
}

func (n *nodeManager) getNode(nodeId uint64) *raft.Member {
	n.nlk.RLock()
	defer n.nlk.RUnlock()

	return n.nodes[nodeId]
}

func (n *nodeManager) Resolve(ctx context.Context, nodeID uint64) (raft.Addr, error) {
	m := n.getNode(nodeID)
	if m == nil {
		return nil, fmt.Errorf("not found target addr, node Id %d", nodeID)
	}

	return nodeAddr{addr: m.Host}, nil
}

type nodeAddr struct {
	addr string
}

func (n nodeAddr) String() string {
	return n.addr
}

const (
	RaftWalCF = "raft-wal"
)

type RaftSnapshotTransmitConfig struct {
	BatchInflightNum  int `json:"batch_inflight_num"`
	BatchInflightSize int `json:"batch_inflight_size"`
}

type raftSnapshot struct {
	*RaftSnapshotTransmitConfig

	appliedIndex uint64
	iterIndex    int
	st           kvstore.Snapshot
	ro           kvstore.ReadOption
	lrs          []kvstore.ListReader
	kvStore      kvstore.Store
}

// ReadBatch read batch data for snapshot transmit
// An io.EOF error should be return when the read end of snapshot
// TODO: limit the snapshot transmitting speed
func (r *raftSnapshot) ReadBatch() (raft.Batch, error) {
	var (
		size  = 0
		batch raft.Batch
	)

	for i := 0; i < r.BatchInflightNum; i++ {
		if size >= r.BatchInflightSize {
			return batch, nil
		}

		kg, vg, err := r.lrs[r.iterIndex].ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			if r.iterIndex == len(r.lrs)-1 {
				return batch, io.EOF
			}
			r.iterIndex++
			return batch, nil
		}

		if batch == nil {
			batch = raftBatch{batch: r.kvStore.NewWriteBatch()}
		}
		batch.Put(kg.Key(), vg.Value())
		size += vg.Size()
	}

	return batch, nil
}

func (r *raftSnapshot) Index() uint64 {
	return r.appliedIndex
}

func (r *raftSnapshot) Close() error {
	for i := range r.lrs {
		r.lrs[i].Close()
	}
	r.st.Close()
	r.ro.Close()
	return nil
}

type raftStorage struct {
	kvStore kvstore.Store
}

func (w *raftStorage) Get(key []byte) (raft.ValGetter, error) {
	return w.kvStore.Get(context.TODO(), RaftWalCF, key, nil)
}

func (w *raftStorage) Iter(prefix []byte) raft.Iterator {
	return raftIterator{lr: w.kvStore.List(context.TODO(), RaftWalCF, prefix, nil, nil)}
}

func (w *raftStorage) NewBatch() raft.Batch {
	return raftBatch{cf: RaftWalCF, batch: w.kvStore.NewWriteBatch()}
}

func (w *raftStorage) Write(b raft.Batch) error {
	return w.kvStore.Write(context.TODO(), b.(raftBatch).batch, nil)
}

type raftIterator struct {
	lr kvstore.ListReader
}

func (i raftIterator) SeekTo(key []byte) {
	i.lr.SeekTo(key)
}

func (i raftIterator) SeekForPrev(prev []byte) error {
	return i.lr.SeekForPrev(prev)
}

func (i raftIterator) ReadNext() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadNext()
}

func (i raftIterator) ReadPrev() (key raft.KeyGetter, val raft.ValGetter, err error) {
	return i.lr.ReadPrev()
}

func (i raftIterator) Close() {
	i.lr.Close()
}

type raftBatch struct {
	cf    kvstore.CF
	batch kvstore.WriteBatch
}

func (t raftBatch) Put(key, value []byte) { t.batch.Put(t.cf, key, value) }

func (t raftBatch) Delete(key []byte) { t.batch.Delete(t.cf, key) }

func (t raftBatch) DeleteRange(start []byte, end []byte) { t.batch.DeleteRange(t.cf, start, end) }

func (t raftBatch) Data() []byte { return t.batch.Data() }

func (t raftBatch) From(data []byte) { t.batch.From(data) }

func (t raftBatch) Close() { t.batch.Close() }
