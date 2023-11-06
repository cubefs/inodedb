package catalog

import (
	"context"
	"hash/crc32"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	"github.com/cubefs/inodedb/shardserver/scalar"
	"github.com/cubefs/inodedb/shardserver/store"
	"github.com/cubefs/inodedb/shardserver/vector"
	"github.com/cubefs/inodedb/util"
	pb "google.golang.org/protobuf/proto"
)

const keyLocksNum = 1024

type (
	ShardBaseConfig struct {
		RaftSnapTransmitConfig RaftSnapshotTransmitConfig `json:"raft_snap_transmit_config"`
		TruncateWalLogInterval uint64                     `json:"truncate_wal_log_interval"`
	}

	shardConfig struct {
		*ShardBaseConfig
		shardInfo   shardInfo
		nodeInfo    *proto.Node
		store       *store.Store
		raftManager raft.Manager
	}
)

func newShard(ctx context.Context, cfg shardConfig) (s *shard, err error) {
	s = &shard{
		shardID:  cfg.shardInfo.ShardId,
		startIno: calculateStartIno(cfg.shardInfo.ShardId),
		store:    cfg.store,
		shardKeys: &shardKeysGenerator{
			shardId: cfg.shardInfo.ShardId,
			spaceId: cfg.shardInfo.Sid,
		},
		cfg: cfg.ShardBaseConfig,
	}
	s.shardMu.shardInfo = cfg.shardInfo

	learner := false
	for _, node := range cfg.shardInfo.Nodes {
		if node.Id == cfg.nodeInfo.Id {
			learner = node.Learner
			break
		}
	}
	// initial members
	members := make([]raft.Member, 0, len(cfg.shardInfo.Nodes))
	for _, node := range cfg.shardInfo.Nodes {
		members = append(members, raft.Member{
			NodeID:  uint64(node.Id),
			Host:    cfg.nodeInfo.Addr + strconv.Itoa(int(cfg.nodeInfo.RaftPort)),
			Type:    raft.MemberChangeType_AddMember,
			Learner: learner,
		})
	}

	s.raftGroup, err = cfg.raftManager.CreateRaftGroup(context.Background(), &raft.GroupConfig{
		ID:      uint64(s.shardID),
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

type shardStats struct {
	leader       uint32
	routeVersion uint64
	inoUsed      uint64
	inoCursor    uint64
	inoLimit     uint64
	nodes        []*persistent.ShardNode
}

type shard struct {
	shardID      uint32
	startIno     uint64
	appliedIndex uint64
	shardMu      struct {
		sync.RWMutex
		shardInfo
		leader uint32
	}
	keyLocks [keyLocksNum]sync.Mutex

	shardKeys   *shardKeysGenerator
	vectorIndex *vector.Index
	scalarIndex *scalar.Index
	store       *store.Store
	raftGroup   raft.Group
	cfg         *ShardBaseConfig
}

func (s *shard) InsertItem(ctx context.Context, i *proto.Item) (uint64, error) {
	ino, err := s.nextIno()
	if err != nil {
		return 0, err
	}

	i.Ino = ino
	data, err := pb.Marshal(i)
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

func (s *shard) UpdateItem(ctx context.Context, updateItem *proto.Item) error {
	if !s.checkInoMatchRange(updateItem.Ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := pb.Marshal(updateItem)
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

func (s *shard) GetItem(ctx context.Context, ino uint64) (*proto.Item, error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	data, err := kvStore.GetRaw(ctx, dataCF, s.shardKeys.encodeInoKey(ino), nil)
	if err != nil {
		return nil, err
	}
	item := &item{}
	if err := item.Unmarshal(data); err != nil {
		return nil, err
	}

	// transform into external item
	fields := internalFieldsToProtoFields(item.Fields)
	return &proto.Item{Ino: item.Ino, Links: item.Links, Fields: fields}, nil
}

func (s *shard) Link(ctx context.Context, l *proto.Link) error {
	if !s.checkInoMatchRange(l.Parent) {
		return apierrors.ErrInoMismatchShardRange
	}
	if !s.checkInoMatchRange(l.Child) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := pb.Marshal(l)
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

func (s *shard) Unlink(ctx context.Context, unlink *proto.Unlink) error {
	if !s.checkInoMatchRange(unlink.Parent) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := pb.Marshal(unlink)
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

func (s *shard) List(ctx context.Context, ino uint64, start string, num uint32) (ret []*proto.Link, err error) {
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
		ret = append(ret, &proto.Link{
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
	replicates := make([]*persistent.ShardNode, len(s.shardMu.Nodes))
	copy(replicates, s.shardMu.Nodes)
	inoLimit := s.shardMu.InoLimit
	routeVersion := s.shardMu.Epoch
	leader := s.shardMu.leader
	s.shardMu.RUnlock()

	return &shardStats{
		leader:       leader,
		routeVersion: routeVersion,
		inoUsed:      atomic.LoadUint64(&s.shardMu.InoUsed),
		inoCursor:    atomic.LoadUint64(&s.shardMu.InoCursor),
		inoLimit:     inoLimit,
		nodes:        replicates,
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
	// do flush job
	if err := s.SaveShardInfo(ctx, true); err != nil {
		return errors.Info(err, "save shard into failed")
	}
	// todo: add vector index dump job

	// truncate raft log finally
	appliedIndex := (*shardSM)(s).getAppliedIndex()
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
	key := make([]byte, shardPrefixSize())
	encodeShardPrefix(s.shardMu.Sid, s.shardID, key)
	value, err := s.shardMu.shardInfo.Marshal()
	if err != nil {
		return err
	}

	return kvStore.SetRaw(ctx, dataCF, key, value, nil)
}

func (s *shard) nextIno() (uint64, error) {
	if atomic.LoadUint64(&s.shardMu.InoUsed) > s.shardMu.InoLimit {
		return 0, apierrors.ErrInodeLimitExceed
	}

	for {
		cur := atomic.LoadUint64(&s.shardMu.InoCursor)
		if cur >= s.startIno+proto.ShardRangeStepSize {
			return 0, apierrors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.shardMu.InoCursor, cur, newId) {
			return newId, nil
		}
	}
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

type shardKeysGenerator struct {
	shardId uint32
	spaceId uint64
}

func (s *shardKeysGenerator) encodeInoKey(ino uint64) []byte {
	key := make([]byte, shardInodePrefixSize()+8)
	encodeShardInodePrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[len(key)-8:])
	return key
}

func (s *shardKeysGenerator) encodeLinkKey(ino uint64, name string) []byte {
	shardDataPrefixSize := shardLinkPrefixSize()
	key := make([]byte, shardDataPrefixSize+8+len(infix)+len(name))
	encodeShardLinkPrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[shardDataPrefixSize:])
	copy(key[shardDataPrefixSize+8:], infix)
	copy(key[shardDataPrefixSize+8+len(infix):], util.StringsToBytes(name))
	return key
}

func (s *shardKeysGenerator) decodeLinkKey(key []byte) (ino uint64, name string) {
	shardDataPrefixSize := shardLinkPrefixSize()
	ino = decodeIno(key[shardDataPrefixSize:])
	nameRaw := make([]byte, len(key[shardDataPrefixSize+8+len(infix):]))
	copy(nameRaw, key[shardDataPrefixSize+8+len(infix):])
	name = util.BytesToString(nameRaw)
	return
}

func (s *shardKeysGenerator) encodeLinkKeyPrefix(ino uint64) []byte {
	shardDataPrefixSize := shardLinkPrefixSize()
	keyPrefix := make([]byte, shardDataPrefixSize+8+len(infix))
	encodeShardLinkPrefix(s.spaceId, s.shardId, keyPrefix)
	encodeIno(ino, keyPrefix[shardDataPrefixSize:])
	copy(keyPrefix[shardDataPrefixSize+8:], infix)
	return keyPrefix
}

func protoFieldsToInternalFields(external []*proto.Field) []*persistent.Field {
	ret := make([]*persistent.Field, len(external))
	for i := range external {
		ret[i] = &persistent.Field{
			Name:  external[i].Name,
			Value: external[i].Value,
		}
	}
	return ret
}

func internalFieldsToProtoFields(internal []*persistent.Field) []*proto.Field {
	ret := make([]*proto.Field, len(internal))
	for i := range internal {
		ret[i] = &proto.Field{
			Name:  internal[i].Name,
			Value: internal[i].Value,
		}
	}
	return ret
}
