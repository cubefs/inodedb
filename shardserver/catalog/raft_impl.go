package catalog

import (
	"context"
	"io"
	"strconv"

	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/raft"
)

const (
	raftWalCF = "raft-wal"
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
	return w.kvStore.Get(context.TODO(), raftWalCF, key, nil)
}

func (w *raftStorage) Iter(prefix []byte) raft.Iterator {
	return raftIterator{lr: w.kvStore.List(context.TODO(), raftWalCF, prefix, nil, nil)}
}

func (w *raftStorage) NewBatch() raft.Batch {
	return raftBatch{cf: raftWalCF, batch: w.kvStore.NewWriteBatch()}
}

func (w *raftStorage) Write(b raft.Batch) error {
	return w.kvStore.Write(context.TODO(), b.(raftBatch).batch, nil)
}

type raftIterator struct {
	lr kvstore.ListReader
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

type addressResolver struct {
	t *transport
}

func (a *addressResolver) Resolve(ctx context.Context, nodeID uint64) (raft.Addr, error) {
	node, err := a.t.GetNode(ctx, uint32(nodeID))
	if err != nil {
		return nil, err
	}
	return nodeAddr{addr: node.Addr + strconv.Itoa(int(node.RaftPort))}, nil
}

type nodeAddr struct {
	addr string
}

func (n nodeAddr) String() string {
	return n.addr
}