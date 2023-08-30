package catalog

import (
	"context"
	"hash/crc32"
	"sync"
	"sync/atomic"

	"github.com/cubefs/inodedb/shardserver/catalog/persistent"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/util/btree"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/scalar"
	"github.com/cubefs/inodedb/shardserver/store"
	"github.com/cubefs/inodedb/shardserver/vector"
	"github.com/cubefs/inodedb/util"
	pb "google.golang.org/protobuf/proto"
)

const keyLocksNum = 1024

type shardConfig struct {
	epoch    uint64
	spaceId  uint64
	shardId  uint32
	inoLimit uint64
	nodes    []uint32
	store    *store.Store
}

func newShard(cfg *shardConfig) *shard {
	shard := &shard{
		shardId:  cfg.shardId,
		epoch:    cfg.epoch,
		inoLimit: cfg.inoLimit,
		spaceId:  cfg.spaceId,
		nodes:    cfg.nodes,
		store:    cfg.store,
		shardRange: &shardRange{
			startIno: uint64(cfg.shardId-1) * proto.ShardRangeStepSize,
		},
	}
	return shard
}

type shardRange struct {
	startIno uint64
}

func (s *shardRange) Less(than btree.Item) bool {
	thanShard := than.(*shardRange)
	return s.startIno < thanShard.startIno
}

func (s *shardRange) Copy() btree.Item {
	return &(*s)
}

type shardStats struct {
	routeVersion uint64
	inoUsed      uint64
	inoCursor    uint64
	inoLimit     uint64
	nodes        []uint32
}

type shard struct {
	shardId     uint32
	inoCursor   uint64
	inoLimit    uint64
	inoUsed     uint64
	spaceId     uint64
	epoch       uint64
	nodes       []uint32
	shardKeys   *shardKeysGenerator
	vectorIndex *vector.Index
	scalarIndex *scalar.Index
	store       *store.Store
	raftGroup   *RaftGroup

	keyLocks [keyLocksNum]sync.Mutex
	lock     sync.RWMutex

	// read only
	*shardRange
}

func (s *shard) InsertItem(ctx context.Context, i *proto.Item) (uint64, error) {
	ino, err := s.nextIno()
	if err != nil {
		return 0, err
	}

	data, err := pb.Marshal(i)
	if err != nil {
		return 0, err
	}
	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpInsertItem,
		Data: data,
	}); err != nil {
		return 0, err
	}

	// TODO: remove this function call after invoke multi raft
	_, err = s.Apply(ctx, RaftOpInsertItem, data, 0)
	return ino, err
}

func (s *shard) UpdateItem(ctx context.Context, updateItem *proto.Item) error {
	if !s.checkInoMatchRange(updateItem.Ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	data, err := pb.Marshal(updateItem)
	if err != nil {
		return err
	}

	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpUpdateItem,
		Data: data,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke multi raft
	_, err = s.Apply(ctx, RaftOpUpdateItem, data, 0)
	return err
}

func (s *shard) DeleteItem(ctx context.Context, ino uint64) error {
	if !s.checkInoMatchRange(ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	data := make([]byte, 8)
	encodeIno(ino, data)
	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpDeleteItem,
		Data: data,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke multi raft
	_, err := s.Apply(ctx, RaftOpDeleteItem, data, 0)
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

	data, err := pb.Marshal(l)
	if err != nil {
		return err
	}

	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpLinkItem,
		Data: data,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke multi raft
	_, err = s.Apply(ctx, RaftOpLinkItem, data, 0)
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

	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpUnlinkItem,
		Data: data,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke multi raft
	_, err = s.Apply(ctx, RaftOpUnlinkItem, data, 0)
	return err
}

func (s *shard) List(ctx context.Context, ino uint64, start string, num uint32) (ret []*proto.Link, err error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	linkKeyPrefix := s.shardKeys.encodeLinkKeyPrefix(ino)
	marker := make([]byte, len(linkKeyPrefix)+len(start))
	copy(marker, linkKeyPrefix)
	copy(marker[len(linkKeyPrefix):], start)

	kvStore := s.store.KVStore()
	lr := kvStore.List(ctx, dataCF, linkKeyPrefix, marker, nil)
	for num == 0 {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, errors.Info(err, "read next link failed")
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

func (s *shard) Stats() *shardStats {
	s.lock.RLock()
	replicates := make([]uint32, len(s.nodes))
	copy(replicates, s.nodes)
	inoLimit := s.inoLimit
	routeVersion := s.epoch
	s.lock.RUnlock()

	return &shardStats{
		routeVersion: routeVersion,
		inoUsed:      atomic.LoadUint64(&s.inoUsed),
		inoCursor:    atomic.LoadUint64(&s.inoCursor),
		inoLimit:     inoLimit,
		nodes:        replicates,
	}
}

func (s *shard) Start() {
	s.raftGroup.Start()
}

func (s *shard) nextIno() (uint64, error) {
	if atomic.LoadUint64(&s.inoUsed) > s.inoLimit {
		return 0, apierrors.ErrInodeLimitExceed
	}

	for {
		cur := atomic.LoadUint64(&s.inoCursor)
		if cur >= s.startIno+proto.ShardRangeStepSize {
			return 0, apierrors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.inoCursor, cur, newId) {
			return newId, nil
		}
	}
}

func (s *shard) checkInoMatchRange(ino uint64) bool {
	return s.startIno >= ino && s.startIno+proto.ShardRangeStepSize > ino
}

func (s *shard) getKeyLock(key []byte) sync.Mutex {
	crc32 := crc32.NewIEEE()
	crc32.Write(key)
	idx := crc32.Sum32() % keyLocksNum
	return s.keyLocks[idx]
}

func (s *shard) increaseInoUsed() {
	atomic.AddUint64(&s.inoUsed, 1)
}

func (s *shard) decreaseInoUsed() {
	for {
		cur := atomic.LoadUint64(&s.inoUsed)
		new := cur - 1
		if atomic.CompareAndSwapUint64(&s.inoUsed, cur, new) {
			return
		}
	}
}

type shardKeysGenerator struct {
	shardId uint32
	spaceId uint64
}

func (s *shardKeysGenerator) encodeInoKey(ino uint64) []byte {
	key := make([]byte, shardDataPrefixSize(s.spaceId, s.shardId)+8)
	encodeShardDataPrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[len(key)-8:])
	return key
}

func (s *shardKeysGenerator) encodeLinkKey(ino uint64, name string) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	key := make([]byte, shardDataPrefixSize+8+len(infix)+len(name))
	encodeShardDataPrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[shardDataPrefixSize:])
	copy(key[shardDataPrefixSize+8:], infix)
	copy(key[shardDataPrefixSize+8+len(infix):], util.StringsToBytes(name))
	return key
}

func (s *shardKeysGenerator) decodeLinkKey(key []byte) (ino uint64, name string) {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	ino = decodeIno(key[shardDataPrefixSize:])
	nameRaw := make([]byte, len(key[shardDataPrefixSize+8+len(infix):]))
	copy(nameRaw, key[shardDataPrefixSize+8+len(infix):])
	name = util.BytesToString(nameRaw)
	return
}

func (s *shardKeysGenerator) encodeLinkKeyPrefix(ino uint64) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	keyPrefix := make([]byte, shardDataPrefixSize+8+len(infix))
	encodeShardDataPrefix(s.spaceId, s.shardId, keyPrefix)
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
