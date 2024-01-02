package master

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/master/catalog"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/raft"
)

const (
	defaultTruncateNumInterval = uint64(50000)
	defaultTruncTimeInterval   = 5
	localCF                    = "local_cf"
)

var (
	applyIndexKey = []byte("raft_apply_index")
	raftMemberKey = []byte("#raft_members")
)

type RaftNodeCfg struct {
	Members              []raft.Member `json:"members"`
	RaftConfig           raft.Config   `json:"raft_config"`
	TruncateNumInterval  uint64        `json:"truncate_num_interval"`
	TruncateTimeInterval uint64        `json:"truncate_time_interval"`
}

type RaftMembers struct {
	Mbs []raft.Member `json:"members"`
}

type raftNode struct {
	sms          map[string]raft.Applier
	store        *store.Store
	appliedIndex uint64
	lastTruncIdx uint64
	nodes        *nodeManager
	raftGroup    raft.Group
	cfg          *RaftNodeCfg
}

func newRaftNode(ctx context.Context, cfg *RaftNodeCfg, kv *store.Store) *raftNode {
	span := trace.SpanFromContextSafe(ctx)

	if cfg.RaftConfig.NodeID == 0 {
		span.Fatalf("node id can't be zero")
	}

	if len(cfg.Members) == 0 {
		span.Fatalf("raft config members can't be empty")
	}

	if cfg.TruncateNumInterval == 0 {
		cfg.TruncateNumInterval = defaultTruncateNumInterval
	}

	if cfg.TruncateNumInterval == 0 {
		cfg.TruncateNumInterval = 5 // minutes
	}

	cfg.RaftConfig.Storage = &raftStorage{kvStore: kv.RaftStore()}
	cfg.RaftConfig.Logger = log.DefaultLogger

	r := &raftNode{
		sms:   make(map[string]raft.Applier),
		store: kv,
		cfg:   cfg,
	}

	r.initNodeManager(ctx)
	if err := r.loadApplyIdx(ctx); err != nil {
		span.Fatalf("load apply index failed, err: %v", err)
	}
	r.lastTruncIdx = r.appliedIndex

	span.Infof("new raftNode success")
	return r
}

func (r *raftNode) createRaftGroup(ctx context.Context, cfg *raft.GroupConfig) raft.Group {
	span := trace.SpanFromContextSafe(ctx)

	manager, err := raft.NewManager(&r.cfg.RaftConfig)
	if err != nil {
		span.Fatalf("new manager failed, err %s", err.Error())
	}

	group, err := manager.CreateRaftGroup(ctx, cfg)
	if err != nil {
		span.Fatalf("create raft group failed, err %v", err)
	}

	r.raftGroup = group
	span.Infof("create raft group success")
	return group
}

func (r *raftNode) start(ctx context.Context) {
	if len(r.nodes.nodes) == 1 {
		r.raftGroup.Campaign(ctx)
	}

	r.waitForRaftStart(ctx)

	go r.truncJob()
}

func (r *raftNode) initNodeManager(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	members, err := r.loadRaftMembers(ctx)
	if err != nil {
		span.Fatalf("get raft members failed, err: %v", err)
	}

	needWrite := false
	if len(members) == 0 {
		needWrite = true
		members = r.cfg.Members
	}

	r.nodes = &nodeManager{
		nodes: map[uint64]string{},
	}

	for _, m := range members {
		r.nodes.addNode(m.NodeID, m.Host)
	}

	if needWrite {
		err = r.persistMembers(ctx, members)
		if err != nil {
			span.Fatalf("persist raft members failed, err: %v", err)
		}
	}

	r.cfg.RaftConfig.Resolver = r.nodes
}

func (r *raftNode) waitForRaftStart(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	start := time.Now()
	span.Info("wait for raft start")
	for {
		err := r.raftGroup.ReadIndex(ctx)
		if err == nil {
			break
		}
		span.Errorf("raftNode read index failed: err %v, cost %d ms", err, time.Since(start).Milliseconds())
	}
	span.Infof("raft start success after %d ms", time.Since(start).Microseconds())
}

func (r *raftNode) truncJob() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	lastTruncTime := time.Now()
	ticker := time.NewTicker(time.Minute)

	applyIdOk := func() bool {
		if r.appliedIndex == r.lastTruncIdx || r.appliedIndex == 0 {
			return false
		}

		if r.appliedIndex%r.cfg.TruncateNumInterval != 0 {
			return false
		}
		return true
	}

	timeOk := func() bool {
		if time.Since(lastTruncTime) > time.Duration(r.cfg.TruncateTimeInterval*uint64(time.Minute)) {
			return true
		}
		return false
	}

	span.Infof("start execute truncate job")

	for range ticker.C {
		if !applyIdOk() && !timeOk() {
			continue
		}

		err := r.persistApplyIdx(ctx)
		if err != nil {
			span.Errorf("perist apply idx failed, err %s", err.Error())
			continue
		}

		if r.appliedIndex > r.cfg.TruncateNumInterval {
			err = r.raftGroup.Truncate(ctx, r.appliedIndex-r.cfg.TruncateNumInterval)
			if err != nil {
				span.Errorf("trunc raft log failed, applyId %d, err %s", r.appliedIndex, err.Error())
				continue
			}
		}

		lastTruncTime = time.Now()
		span.Infof("execute trunc success, applyId %d", r.appliedIndex)
	}
}

func (r *raftNode) loadApplyIdx(ctx context.Context) error {
	val, err := r.store.KVStore().GetRaw(ctx, localCF, applyIndexKey, nil)
	if err == kvstore.ErrNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	if len(val) == 0 {
		return nil
	}

	if len(val) != 8 {
		return fmt.Errorf("apply idx not write, size %d, data %s", len(val), string(val))
	}

	r.appliedIndex = binary.BigEndian.Uint64(val[:8])
	return nil
}

func (r *raftNode) persistApplyIdx(ctx context.Context) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, r.appliedIndex)
	if err := r.store.KVStore().SetRaw(ctx, localCF, applyIndexKey, val, nil); err != nil {
		return err
	}
	return nil
}

func (r *raftNode) loadRaftMembers(ctx context.Context) ([]raft.Member, error) {
	val, err := r.store.KVStore().GetRaw(ctx, localCF, raftMemberKey, nil)

	if err == kvstore.ErrNotFound {
		return []raft.Member{}, nil
	}

	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, nil
	}

	mbrs := &RaftMembers{}
	err = json.Unmarshal(val, mbrs)
	if err != nil {
		return nil, err
	}

	return mbrs.Mbs, nil
}

func (r *raftNode) persistMembers(ctx context.Context, members []raft.Member) (err error) {
	mbrs := &RaftMembers{}
	mbrs.Mbs = append(mbrs.Mbs, members...)
	val, err := json.Marshal(mbrs)
	if err != nil {
		return err
	}
	if err = r.store.KVStore().SetRaw(ctx, localCF, raftMemberKey, val, nil); err != nil {
		return err
	}
	return nil
}

func (r *raftNode) addApplier(module string, a raft.Applier) {
	r.sms[module] = a
}

func (r *raftNode) getApplyID() uint64 {
	return r.appliedIndex
}

func (r *raftNode) getMembers() []raft.Member {
	return r.nodes.getMembers()
}

func (r *raftNode) Apply(cxt context.Context, pd []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	// span := trace.SpanFromContext(cxt)
	rets = make([]interface{}, len(pd))

	for i := range pd {
		pdi := pd[i]
		mod := pdi.Module
		// span, _ := trace.StartSpanFromContextWithTraceID(cxt, "apply", string(pdi.Context))
		// span.Debugf("receive apply req, module %s, op %d, data %s", string(pdi.Module), pdi.Op, string(pdi.Data))
		sm := r.sms[string(mod)]
		if sm == nil {
			panic(fmt.Errorf("target mode not exist, mod %s, op %d", mod, pdi.Op))
		}

		newRet, err := sm.Apply(cxt, pdi, 0)
		if err != nil {
			return nil, err
		}

		rets[i] = newRet
		// span.Debugf("finish raft op %d, mod %s", pdi.Op, string(pdi.Module))
	}

	r.appliedIndex = index
	return rets, nil
}

func (r *raftNode) LeaderChange(peerID uint64) error {
	for _, sm := range r.sms {
		err := sm.LeaderChange(peerID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *raftNode) ApplyMemberChange(cc *raft.Member, index uint64) error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	switch cc.Type {
	case raft.MemberChangeType_AddMember:
		oldAddr := r.nodes.getNode(cc.NodeID)
		if oldAddr == "" {
			r.nodes.addNode(cc.NodeID, cc.Host)
		}
	case raft.MemberChangeType_RemoveMember:
		r.nodes.removeNode(cc.NodeID)
	}

	err := r.persistMembers(ctx, r.nodes.getMembers())
	if err != nil {
		span.Errorf("perist members failed, err %s", err.Error())
		return err
	}

	return nil
}

func (r *raftNode) Snapshot() raft.Snapshot {
	kvStore := r.store.KVStore()
	appliedIndex := r.appliedIndex
	kvSnap := kvStore.NewSnapshot()
	readOpt := kvStore.NewReadOption()
	readOpt.SetSnapShot(kvSnap)

	// create cf list reader
	lrs := make([]kvstore.ListReader, 0)
	for _, cf := range []kvstore.CF{catalog.CF, idgenerator.CF, cluster.CF} {
		lrs = append(lrs, kvStore.List(context.Background(), cf, nil, nil, readOpt))
	}

	return &raftSnapshot{
		appliedIndex: appliedIndex,
		st:           kvSnap,
		ro:           readOpt,
		lrs:          lrs,
		kvStore:      kvStore,
	}
}

func (r *raftNode) ApplySnapshot(snap raft.Snapshot) error {
	// todo: clear all data with shard prefix

	defer snap.Close()
	kvStore := r.store.KVStore()
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	for {
		batch, err := snap.ReadBatch()
		if err != nil && err != io.EOF {
			span.Errorf("read data from snapshot failed, err %s", err.Error())
			return err
		}

		if batch != nil {
			if err := kvStore.Write(ctx, batch.(raftBatch).batch, nil); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	r.appliedIndex = snap.Index()
	err := r.persistApplyIdx(ctx)
	if err != nil {
		span.Errorf("persist applied index failed, idx %d, err %s", r.appliedIndex, err.Error())
		return err
	}

	return nil
}

type nodeManager struct {
	nodes map[uint64]string
	nlk   sync.RWMutex
}

// todo presist after update
func (n *nodeManager) addNode(nodeId uint64, addr string) {
	n.nlk.Lock()
	defer n.nlk.Unlock()

	n.nodes[nodeId] = addr
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
	for id, m := range n.nodes {
		mems = append(mems, raft.Member{
			NodeID: id,
			Host:   m,
		})
	}
	return mems
}

func (n *nodeManager) getNode(nodeId uint64) string {
	n.nlk.RLock()
	defer n.nlk.RUnlock()

	return n.nodes[nodeId]
}

func (n *nodeManager) Resolve(ctx context.Context, nodeID uint64) (raft.Addr, error) {
	addr := n.getNode(nodeID)
	if addr == "" {
		return nil, fmt.Errorf("not found target addr, node Id %d", nodeID)
	}

	return nodeAddr{addr: addr}, nil
}

type nodeAddr struct {
	addr string
}

func (n nodeAddr) String() string {
	return n.addr
}

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
