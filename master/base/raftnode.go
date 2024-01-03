package base

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
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/raft"
)

var applyTaskPool = taskpool.New(3, 3)
type RaftMembers struct {
	Mbs []raft.Member `json:"members"`
}
type RaftNodeCfg struct {
	Members              []raft.Member `json:"members"`
	RaftConfig           raft.Config   `json:"raft_config"`
	TruncateNumInterval  uint64        `json:"truncate_num_interval"`
	TruncateTimeInterval uint64        `json:"truncate_time_interval"`
}

type raftNode struct {
	sms          map[string]Applier
	store        *store.Store
	appliedIndex uint64
	lastTruncIdx uint64
	nodes        *nodeManager
	raftGroup    raft.Group
	cfg          *RaftNodeCfg
}

func NewRaftNode(ctx context.Context, cfg *RaftNodeCfg, kv *store.Store) *raftNode {
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
		sms:   make(map[string]Applier),
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

func (r *raftNode) CreateRaftGroup(ctx context.Context, cfg *raft.GroupConfig) RaftGroup {
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
	return &raftServer{
		Group: group,
		id:    r.cfg.RaftConfig.NodeID,
	}
}

func (r *raftNode) Start(ctx context.Context) {
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
		nodes: map[uint64]*raft.Member{},
	}

	for _, m := range members {
		r.nodes.addNode(m.NodeID, m)
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
	val, err := r.store.KVStore().GetRaw(ctx, LocalCF, applyIndexKey, nil)
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
	if err := r.store.KVStore().SetRaw(ctx, LocalCF, applyIndexKey, val, nil); err != nil {
		return err
	}
	return nil
}

func (r *raftNode) loadRaftMembers(ctx context.Context) ([]raft.Member, error) {
	val, err := r.store.KVStore().GetRaw(ctx, LocalCF, raftMemberKey, nil)

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
	if err = r.store.KVStore().SetRaw(ctx, LocalCF, raftMemberKey, val, nil); err != nil {
		return err
	}
	return nil
}

func (r *raftNode) RegisterApplier(a interface{}) {
	applier := a.(Applier)
	r.sms[applier.GetModule()] = applier
}

func (r *raftNode) GetApplyID() uint64 {
	return r.appliedIndex
}

func (r *raftNode) GetMembers() []raft.Member {
	return r.nodes.getMembers()
}

func (r *raftNode) Apply(ctx context.Context, pds []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	// span := trace.SpanFromContext(cxt)
	rets = make([]interface{}, 0, len(pds))
	span, _ := trace.StartSpanFromContext(ctx, "")
	moduleData := map[string][]raft.ProposalData{}
	errs := map[string]error{}
	lk := sync.Mutex{}

	for _, p := range pds {
		mod := string(p.Module)
		datas := moduleData[mod]
		if datas == nil {
			datas = []raft.ProposalData{p}
			moduleData[mod] = datas
		} else {
			moduleData[mod] = append(datas, p)
		}
	}

	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(len(moduleData))

	for m := range moduleData {
		mod := m
		applyTaskPool.Run(func() {
			defer wg.Done()

			sm := r.sms[mod]
			if sm == nil {
				panic(fmt.Errorf("target mode not exist, mod %s", m))
			}
			datas := moduleData[mod]
			lRets, err1 := sm.Apply(ctx, datas)
			if err1 != nil {
				span.Errorf("apply module %s error: %s", mod, err1.Error())
				errs[mod] = err1
				return
			}

			span.Debugf("apply module %s success, data cnt %d, ret cnt %d", mod, len(datas), len(lRets))

			lk.Lock()
			defer lk.Unlock()
			rets = append(rets, lRets...)
		})
	}

	wg.Wait()

	for _, err := range errs {
		return nil, err
	}

	span.Debugf("apply success, total data: %d, rets (%d), module cnt (%d) apply cost: %dus, applyIdx %d",
		len(pds), len(rets), len(moduleData), time.Since(start).Microseconds(), index)

	r.appliedIndex = index
	return rets, nil
}

func (r *raftNode) LeaderChange(peerID uint64) error {
	span, _ := trace.StartSpanFromContext(context.Background(), "")
	span.Infof("leader change signal, local %v, leader %d", r.cfg.RaftConfig.NodeID, peerID)
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
	span.Infof("recive member change: %v, local members: %s", cc, r.nodes.String())

	switch cc.Type {
	case raft.MemberChangeType_AddMember:
		m := r.nodes.getNode(cc.NodeID)
		if m == nil {
			r.nodes.addNode(cc.NodeID, *cc)
		}
	case raft.MemberChangeType_RemoveMember:
		r.nodes.removeNode(cc.NodeID)
	}

	err := r.persistMembers(ctx, r.nodes.getMembers())
	if err != nil {
		span.Errorf("perist members failed, err %s", err.Error())
		return err
	}

	return r.persistApplyIdx(ctx)
}

func (r *raftNode) Snapshot() raft.Snapshot {
	kvStore := r.store.KVStore()
	appliedIndex := r.appliedIndex
	kvSnap := kvStore.NewSnapshot()
	readOpt := kvStore.NewReadOption()
	readOpt.SetSnapShot(kvSnap)

	// create cf list reader
	lrs := make([]kvstore.ListReader, 0)
	var cfs []kvstore.CF
	for _, s := range r.sms {
		cfs = append(cfs, s.GetCF()...)
	}

	for _, cf := range cfs {
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
