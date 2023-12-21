package catalog

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	"github.com/cubefs/inodedb/shardserver/scalar"
	"github.com/cubefs/inodedb/shardserver/store"
	"github.com/cubefs/inodedb/shardserver/vector"
	"github.com/cubefs/inodedb/util"
)

const keyLocksNum = 1024

type (
	ShardBaseConfig struct {
		RaftSnapTransmitConfig RaftSnapshotTransmitConfig `json:"raft_snap_transmit_config"`
		TruncateWalLogInterval uint64                     `json:"truncate_wal_log_interval"`
		InoAllocRangeStep      uint64                     `json:"ino_alloc_range_step"`
	}

	shardConfig struct {
		*ShardBaseConfig
		diskID      proto.DiskID
		shardInfo   shardInfo
		nodeInfo    *proto.Node
		store       *store.Store
		raftManager raft.Manager
	}

	shardStats struct {
		leader    proto.DiskID
		epoch     uint64
		inoUsed   uint64
		inoCursor uint64
		inoLimit  uint64
		nodes     []persistent.ShardNode
	}
)

type inodeRange struct {
	start uint64
	end   uint64
}

func (i inodeRange) isEmpty() bool {
	return i.start == 0 && i.end == 0
}

func newShard(ctx context.Context, cfg shardConfig) (s *shard, err error) {
	span := trace.SpanFromContext(ctx)
	span.Infof("new shard with config: %+v", cfg)

	s = &shard{
		sid:      cfg.shardInfo.Sid,
		shardID:  cfg.shardInfo.ShardID,
		diskID:   cfg.diskID,
		startIno: calculateStartIno(cfg.shardInfo.ShardID),

		store: cfg.store,
		shardKeys: &shardKeysGenerator{
			shardID: cfg.shardInfo.ShardID,
			spaceID: cfg.shardInfo.Sid,
		},
		cfg: cfg.ShardBaseConfig,
	}
	s.shardMu.shardInfo = cfg.shardInfo

	learner := false
	for _, node := range cfg.shardInfo.Nodes {
		if node.DiskID == cfg.diskID {
			learner = node.Learner
			break
		}
	}
	// initial members
	members := make([]raft.Member, 0, len(cfg.shardInfo.Nodes))
	for _, node := range cfg.shardInfo.Nodes {
		members = append(members, raft.Member{
			NodeID:  uint64(node.DiskID),
			Host:    cfg.nodeInfo.Addr + ":" + strconv.Itoa(int(cfg.nodeInfo.RaftPort)),
			Type:    raft.MemberChangeType_AddMember,
			Learner: learner,
		})
	}
	span.Debugf("shard members: %+v", members)

	s.raftGroup, err = cfg.raftManager.CreateRaftGroup(context.Background(), &raft.GroupConfig{
		ID:      encodeShardID(cfg.shardInfo.Sid, cfg.shardInfo.ShardID),
		Applied: cfg.shardInfo.AppliedIndex,
		Members: members,
		SM:      (*shardSM)(s),
	})
	if err != nil {
		return
	}

	if len(members) == 1 {
		err = s.raftGroup.Campaign(ctx)
	}

	return
}

type shard struct {
	sid      proto.Sid
	shardID  proto.ShardID
	diskID   proto.DiskID
	startIno uint64

	shardMu struct {
		sync.RWMutex
		shardInfo
		leader proto.DiskID
	}
	inoRange atomic.Value
	sf       singleflight.Group
	keyLocks [keyLocksNum]sync.Mutex

	shardKeys   *shardKeysGenerator
	vectorIndex *vector.Index
	scalarIndex *scalar.Index
	store       *store.Store
	raftGroup   raft.Group
	cfg         *ShardBaseConfig
}

func (s *shard) InsertItem(ctx context.Context, i proto.Item) (uint64, error) {
	if !s.isLeader() {
		return 0, apierrors.ErrNotLeader
	}
	ino, err := s.nextIno(ctx)
	if err != nil {
		return 0, err
	}

	i.Ino = ino
	data, err := i.Marshal()
	if err != nil {
		return 0, err
	}

	proposalData := raft.ProposalData{
		Op:   RaftOpInsertItem,
		Data: data,
	}
	if _, err := s.raftGroup.Propose(ctx, &proposalData); err != nil {
		return 0, err
	}

	return ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, updateItem proto.Item) error {
	if !s.isLeader() {
		return apierrors.ErrNotLeader
	}
	if !s.checkInoMatchRange(updateItem.Ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := updateItem.Marshal()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   RaftOpUpdateItem,
		Data: data,
	}
	_, err = s.raftGroup.Propose(ctx, &proposalData)

	return err
}

func (s *shard) DeleteItem(ctx context.Context, ino uint64) error {
	if !s.isLeader() {
		return apierrors.ErrNotLeader
	}
	if !s.checkInoMatchRange(ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	data := make([]byte, 8)
	encodeIno(ino, data)
	proposalData := raft.ProposalData{
		Op:   RaftOpDeleteItem,
		Data: data,
	}
	_, err := s.raftGroup.Propose(ctx, &proposalData)
	return err
}

func (s *shard) GetItem(ctx context.Context, ino uint64) (protoItem proto.Item, err error) {
	if !s.checkInoMatchRange(ino) {
		err = apierrors.ErrInoMismatchShardRange
		return
	}

	kvStore := s.store.KVStore()
	data, err := kvStore.GetRaw(ctx, dataCF, s.shardKeys.encodeInoKey(ino), nil)
	if err != nil {
		return
	}
	item := item{}
	if err = item.Unmarshal(data); err != nil {
		return
	}

	protoItem.Ino = item.Ino
	protoItem.Links = item.Links
	// transform into external item
	protoItem.Fields = internalFieldsToProtoFields(item.Fields)
	return
}

func (s *shard) Link(ctx context.Context, l proto.Link) error {
	if !s.isLeader() {
		return apierrors.ErrNotLeader
	}
	if !s.checkInoMatchRange(l.Parent) || !s.checkInoMatchRange(l.Child) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := l.Marshal()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   RaftOpLinkItem,
		Data: data,
	}
	_, err = s.raftGroup.Propose(ctx, &proposalData)
	return err
}

func (s *shard) Unlink(ctx context.Context, unlink proto.Unlink) error {
	if !s.isLeader() {
		return apierrors.ErrNotLeader
	}
	if !s.checkInoMatchRange(unlink.Parent) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := unlink.Marshal()
	if err != nil {
		return err
	}

	proposalData := raft.ProposalData{
		Op:   RaftOpUnlinkItem,
		Data: data,
	}
	_, err = s.raftGroup.Propose(ctx, &proposalData)
	return err
}

func (s *shard) List(ctx context.Context, ino uint64, start string, num uint32) (ret []proto.Link, err error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	linkKeyPrefix := s.shardKeys.encodeLinkKeyPrefix(ino)
	var marker []byte
	if start != "" {
		marker = make([]byte, len(linkKeyPrefix)+len(start))
		copy(marker, linkKeyPrefix)
		copy(marker[len(linkKeyPrefix):], start)
	}

	kvStore := s.store.KVStore()
	lr := kvStore.List(ctx, dataCF, linkKeyPrefix, marker, nil)
	for num != 0 {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, errors.Info(err, "read next link failed")
		}

		if kg == nil || vg == nil {
			return ret, nil
		}

		link := &link{}
		if err := link.Unmarshal(vg.Value()); err != nil {
			kg.Close()
			vg.Close()
			return nil, errors.Info(err, "unmarshal link data failed")
		}

		link.Parent = ino
		_, link.Name = s.shardKeys.decodeLinkKey(kg.Key())
		fields := internalFieldsToProtoFields(link.Fields)
		// transform to external link
		ret = append(ret, proto.Link{
			Parent: link.Parent,
			Name:   link.Name,
			Child:  link.Child,
			Fields: fields,
		})
		kg.Close()
		vg.Close()
		num--
	}
	return
}

func (s *shard) UpdateShard(ctx context.Context, shardInfo *persistent.ShardInfo) error {
	s.shardMu.Lock()
	oldEpoch := s.shardMu.Epoch
	s.shardMu.Epoch = shardInfo.Epoch
	if err := s.SaveShardInfo(ctx, false); err != nil {
		s.shardMu.Epoch = oldEpoch
		return err
	}
	s.shardMu.Unlock()

	return nil
}

func (s *shard) GetEpoch() uint64 {
	s.shardMu.RLock()
	epoch := s.shardMu.Epoch
	s.shardMu.RUnlock()
	return epoch
}

func (s *shard) Stats() *shardStats {
	s.shardMu.RLock()
	replicates := make([]persistent.ShardNode, len(s.shardMu.Nodes))
	copy(replicates, s.shardMu.Nodes)
	inoLimit := s.shardMu.InoLimit
	epoch := s.shardMu.Epoch
	leader := s.shardMu.leader
	s.shardMu.RUnlock()

	return &shardStats{
		leader:    leader,
		epoch:     epoch,
		inoUsed:   atomic.LoadUint64(&s.shardMu.InoUsed),
		inoCursor: atomic.LoadUint64(&s.shardMu.InoCursor),
		inoLimit:  inoLimit,
		nodes:     replicates,
	}
}

func (s *shard) Start() {
}

func (s *shard) Stop() {
	// TODO: stop all operation on this shard
}

func (s *shard) Close() {
	// TODO: wait all operation done on this shard and then close shard, ensure memory safe

	s.raftGroup.Close()
}

// Checkpoint do checkpoint job with raft group
// we should do any memory flush job or dump worker here
func (s *shard) Checkpoint(ctx context.Context) error {
	appliedIndex := (*shardSM)(s).getAppliedIndex()

	// todo: add vector index dump job

	// do flush job
	if err := s.SaveShardInfo(ctx, true); err != nil {
		return errors.Info(err, "save shard into failed")
	}

	// truncate raft log finally
	if appliedIndex > s.cfg.TruncateWalLogInterval {
		return s.raftGroup.Truncate(ctx, appliedIndex-s.cfg.TruncateWalLogInterval)
	}

	return nil
}

func (s *shard) SaveShardInfo(ctx context.Context, withLock bool) error {
	if withLock {
		s.shardMu.Lock()
		defer s.shardMu.Unlock()
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeShardInfoKey()
	value, err := s.shardMu.shardInfo.Marshal()
	if err != nil {
		return err
	}

	if err := kvStore.SetRaw(ctx, dataCF, key, value, nil); err != nil {
		return err
	}
	// flush data column family after save shard kv info
	// todo: use other solution?
	return kvStore.FlushCF(ctx, dataCF)
}

func (s *shard) GetVector(ctx context.Context, id uint64) ([]float32, error) {
	kvStore := s.store.KVStore()
	vg, err := kvStore.Get(ctx, dataCF, s.shardKeys.encodeVectorKey(id), nil)
	if err != nil {
		return nil, err
	}

	defer vg.Close()
	embedding := &embedding{}
	if err := embedding.Unmarshal(vg.Value()); err != nil {
		return nil, err
	}

	return embedding.Elements, nil
}

func (s *shard) GetVectors(ctx context.Context, ids []uint64) ([][]float32, error) {
	kvStore := s.store.KVStore()
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = s.shardKeys.encodeVectorKey(ids[i])
	}
	vgs, err := kvStore.MultiGet(ctx, dataCF, keys, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		for _, vg := range vgs {
			vg.Close()
		}
	}()

	ret := make([][]float32, len(vgs))
	for i := range ret {
		if vgs[i] == nil {
			continue
		}
		embedding := &embedding{}
		if err := embedding.Unmarshal(vgs[i].Value()); err != nil {
			return nil, err
		}
		ret[i] = embedding.Elements
	}
	return ret, nil
}

func (s *shard) nextIno(ctx context.Context) (uint64, error) {
	if atomic.LoadUint64(&s.shardMu.InoUsed) > s.shardMu.InoLimit {
		return 0, apierrors.ErrInodeLimitExceed
	}

	inoRange, isInit := s.inoRange.Load().(*inodeRange)
	for {
	RETRY:
		// try cas get inode
		if isInit {
			cur := atomic.LoadUint64(&inoRange.start)
			if cur < inoRange.end {
				newId := cur + 1
				if atomic.CompareAndSwapUint64(&inoRange.start, cur, newId) {
					return newId, nil
				}
				goto RETRY
			}
		}

		if _, err, _ := s.sf.Do("alloc-ino-range", func() (interface{}, error) {
			// check inode range has been updated or not
			tmp := inoRange
			if inoRange, ok := s.inoRange.Load().(*inodeRange); ok && inoRange != tmp {
				return nil, nil
			}

			// alloc new range and swap atomic value
			newRange, err := s.allocInoRange(ctx)
			if err != nil {
				return nil, err
			}
			if newRange.isEmpty() {
				return nil, apierrors.ErrInodeLimitExceed
			}

			s.inoRange.Store(&newRange)
			inoRange = &newRange
			isInit = true
			return nil, nil
		}); err != nil {
			return 0, err
		}
	}
}

func (s *shard) allocInoRange(ctx context.Context) (inodeRange, error) {
	if atomic.LoadUint64(&s.shardMu.InoUsed) > s.shardMu.InoLimit {
		return inodeRange{}, apierrors.ErrInodeLimitExceed
	}

	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, s.cfg.InoAllocRangeStep)

	ret, err := s.raftGroup.Propose(ctx, &raft.ProposalData{
		Op:   RaftOpAllocInoRange,
		Data: data,
	})
	if err != nil {
		return inodeRange{}, err
	}

	return ret.Data.(inodeRange), nil
}

func (s *shard) checkInoMatchRange(ino uint64) bool {
	return s.startIno <= ino && s.startIno+proto.ShardRangeStepSize > ino
}

func (s *shard) getKeyLock(key []byte) sync.Mutex {
	crc32 := crc32.NewIEEE()
	crc32.Write(key)
	idx := crc32.Sum32() % keyLocksNum
	return s.keyLocks[idx]
}

func (s *shard) isLeader() bool {
	s.shardMu.RLock()
	isLeader := s.shardMu.leader == s.diskID
	s.shardMu.RUnlock()

	return isLeader
}

type shardKeysGenerator struct {
	shardID uint32
	spaceID uint64
}

func (s *shardKeysGenerator) encodeInoKey(ino uint64) []byte {
	key := make([]byte, shardInodePrefixSize()+8)
	encodeShardInodePrefix(s.spaceID, s.shardID, key)
	encodeIno(ino, key[len(key)-8:])
	return key
}

func (s *shardKeysGenerator) encodeLinkKey(ino uint64, name string) []byte {
	shardDataPrefixSize := shardLinkPrefixSize()
	key := make([]byte, shardDataPrefixSize+8+len(shardSuffix)+len(name))
	encodeShardLinkPrefix(s.spaceID, s.shardID, key)
	encodeIno(ino, key[shardDataPrefixSize:])
	copy(key[shardDataPrefixSize+8:], shardSuffix)
	copy(key[shardDataPrefixSize+8+len(shardSuffix):], util.StringsToBytes(name))
	return key
}

func (s *shardKeysGenerator) decodeLinkKey(key []byte) (ino uint64, name string) {
	shardDataPrefixSize := shardLinkPrefixSize()
	ino = decodeIno(key[shardDataPrefixSize:])
	nameRaw := make([]byte, len(key[shardDataPrefixSize+8+len(shardSuffix):]))
	copy(nameRaw, key[shardDataPrefixSize+8+len(shardSuffix):])
	name = util.BytesToString(nameRaw)
	return
}

func (s *shardKeysGenerator) encodeLinkKeyPrefix(ino uint64) []byte {
	shardDataPrefixSize := shardLinkPrefixSize()
	keyPrefix := make([]byte, shardDataPrefixSize+8+len(shardSuffix))
	encodeShardLinkPrefix(s.spaceID, s.shardID, keyPrefix)
	encodeIno(ino, keyPrefix[shardDataPrefixSize:])
	copy(keyPrefix[shardDataPrefixSize+8:], shardSuffix)
	return keyPrefix
}

func (s *shardKeysGenerator) encodeVectorKey(vid uint64) []byte {
	key := make([]byte, shardVectorPrefixSize()+8)
	encodeShardVectorPrefix(s.spaceID, s.shardID, key)
	encodeIno(vid, key[len(key)-8:])
	return key
}

func (s *shardKeysGenerator) encodeShardInfoKey() []byte {
	key := make([]byte, shardInfoPrefixSize())
	encodeShardInfoPrefix(s.spaceID, s.shardID, key)
	return key
}

func (s *shardKeysGenerator) encodeShardPrefix() []byte {
	key := make([]byte, shardPrefixSize())
	encodeShardPrefix(s.spaceID, s.shardID, key)
	return key
}

func (s *shardKeysGenerator) encodeShardMaxPrefix() []byte {
	key := make([]byte, shardMaxPrefixSize())
	encodeShardMaxPrefix(s.spaceID, s.shardID, key)
	return key
}

func protoFieldsToInternalFields(external []proto.Field) []persistent.Field {
	ret := make([]persistent.Field, len(external))
	for i := range external {
		ret[i] = persistent.Field{
			Name:  external[i].Name,
			Value: external[i].Value,
		}
	}
	return ret
}

func internalFieldsToProtoFields(internal []persistent.Field) []proto.Field {
	ret := make([]proto.Field, len(internal))
	for i := range internal {
		ret[i] = proto.Field{
			Name:  internal[i].Name,
			Value: internal[i].Value,
		}
	}
	return ret
}

func calculateStartIno(shardID proto.ShardID) uint64 {
	return uint64(shardID-1)*proto.ShardRangeStepSize + 1
}
