package catalog

import (
	"context"
	"hash/crc32"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/inodedb/common/kvstore"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shard/scalar"
	"github.com/cubefs/inodedb/shard/store"
	"github.com/cubefs/inodedb/shard/vector"
	"github.com/cubefs/inodedb/util"
)

const keyLocksNum = 1024

type shardConfig struct {
	routeVersion uint64
	spaceId      uint64
	shardId      uint32
	inoLimit     uint64
	nodes        map[uint32]string
	store        *store.Store
}

func newShard(cfg *shardConfig) *shard {
	shard := &shard{
		shardId:      cfg.shardId,
		routeVersion: cfg.routeVersion,
		inoLimit:     cfg.inoLimit,
		spaceId:      cfg.spaceId,
		nodes:        cfg.nodes,
		store:        cfg.store,
		shardRange: &shardRange{
			startIno: uint64(cfg.shardId-1) * proto.ShardFixedRange,
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
	shardId      uint32
	inoCursor    uint64
	inoLimit     uint64
	inoUsed      uint64
	spaceId      uint64
	routeVersion uint64
	nodes        map[uint32]string
	shardKeys    *shardKeys
	vectorIndex  *vector.Index
	scalarIndex  *scalar.Index
	store        *store.Store
	raftGroup    *RaftGroup

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

	// transform into internal item
	fields := externalFieldsToInternalFields(i.Fields)
	data, err := (&item{
		ino:    ino,
		links:  i.Links,
		fields: fields,
	}).Marshal()
	if err != nil {
		return 0, err
	}

	if _, err := s.raftGroup.Propose(ctx, &RaftProposeRequest{
		Op:   RaftOpInsertItem,
		Data: data,
	}); err != nil {
		return 0, err
	}

	return ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, updateItem *proto.Item) error {
	if !s.checkInoMatchRange(updateItem.Ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeInoKey(updateItem.Ino)
	data, err := kvStore.GetRaw(ctx, dataCF, key, nil)
	if err != nil {
		return err
	}
	item := &item{}
	if err := item.Unmarshal(data); err != nil {
		return err
	}

	fieldMap := make(map[string]int)
	for i := range item.fields {
		fieldMap[item.fields[i].name] = i
	}
	for _, updateField := range updateItem.Fields {
		// update existed field or insert new field
		if idx, ok := fieldMap[updateField.Name]; ok {
			item.fields[idx].value = updateField.Value
			continue
		}
		item.fields = append(item.fields, field{name: updateField.Name, value: updateField.Value})
	}
	data, err = item.Marshal()
	if err != nil {
		return err
	}

	// TODO: move into raft apply progress
	return kvStore.SetRaw(ctx, dataCF, key, data, nil)
}

func (s *shard) DeleteItem(ctx context.Context, ino uint64) error {
	if !s.checkInoMatchRange(ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	// TODO: move into raft apply progress
	kvStore := s.store.KVStore()
	if err := kvStore.Delete(ctx, dataCF, s.shardKeys.encodeInoKey(ino), nil); err != nil {
		return err
	}

	s.decreaseInoUsed()
	return nil
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
	fields := internalFieldsToExternalFields(item.fields)
	return &proto.Item{Ino: item.ino, Links: item.links, Fields: fields}, nil
}

func (s *shard) Link(ctx context.Context, l *proto.Link) error {
	if !s.checkInoMatchRange(l.Parent) {
		return apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	// transform into internal link
	fields := externalFieldsToInternalFields(l.Fields)
	link := &link{
		parent: l.Parent,
		name:   l.Name,
		child:  l.Child,
		fields: fields,
	}
	linkData, err := link.Marshal()
	if err != nil {
		return errors.Info(err, "marshal link data failed")
	}

	// TODO: move into raft apply progress
	pKey := s.shardKeys.encodeInoKey(link.parent)
	linkKey := s.shardKeys.encodeLinkKey(link.parent, link.name)
	lock := s.getKeyLock(pKey)
	lock.Lock()
	defer lock.Unlock()

	vg, err := kvStore.Get(ctx, dataCF, linkKey, nil)
	if err != nil && err != kvstore.ErrNotFound {
		return errors.Info(err, "get link data failed")
	}
	// independent check
	if err == nil {
		vg.Close()
		return nil
	}

	raw, err := kvStore.GetRaw(ctx, dataCF, pKey, nil)
	if err != nil {
		return errors.Info(err, "get parent item data failed")
	}
	pItem := &item{}
	if err := pItem.Unmarshal(raw); err != nil {
		return errors.Info(err, "unmarshal parent item data failed")
	}
	pItem.links += 1
	pData, err := pItem.Marshal()
	if err != nil {
		return errors.Info(err, "marshal parent item data failed")
	}

	batch := kvStore.NewWriteBatch()
	batch.Put(dataCF, linkKey, linkData)
	batch.Put(dataCF, pKey, pData)
	return kvStore.Write(ctx, batch, nil)
}

func (s *shard) Unlink(ctx context.Context, ino uint64, name string) error {
	if !s.checkInoMatchRange(ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	// TODO: move into raft apply progress
	kvStore := s.store.KVStore()
	pKey := s.shardKeys.encodeInoKey(ino)
	linkKey := s.shardKeys.encodeLinkKey(ino, name)
	lock := s.getKeyLock(pKey)
	lock.Lock()
	defer lock.Unlock()

	vg, err := kvStore.Get(ctx, dataCF, linkKey, nil)
	if err != nil && err != kvstore.ErrNotFound {
		return errors.Info(err, "get link data failed")
	}
	// independent check
	if err == kvstore.ErrNotFound {
		return nil
	}
	vg.Close()

	raw, err := kvStore.GetRaw(ctx, dataCF, pKey, nil)
	if err != nil {
		return errors.Info(err, "get parent item data failed")
	}
	pItem := &item{}
	if err := pItem.Unmarshal(raw); err != nil {
		return errors.Info(err, "unmarshal parent item data failed")
	}
	pItem.links -= 1
	pData, err := pItem.Marshal()
	if err != nil {
		return errors.Info(err, "marshal parent item data failed")
	}

	batch := kvStore.NewWriteBatch()
	batch.Delete(dataCF, linkKey)
	batch.Put(dataCF, pKey, pData)
	return kvStore.Write(ctx, batch, nil)
}

func (s *shard) List(ctx context.Context, ino uint64, start string, num uint32) (ret []*proto.Link, err error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	linkKeyPrefix := s.shardKeys.encodeLinkKeyPrefix(ino)
	marker := make([]byte, len(linkKeyPrefix)+len(start))
	copy(marker, linkKeyPrefix)
	copy(marker[len(linkKeyPrefix):], start)
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

		link.parent = ino
		_, link.name = s.shardKeys.decodeLinkKey(kg.Key())
		fields := internalFieldsToExternalFields(link.fields)
		// transform to external link
		ret = append(ret, &proto.Link{
			Parent: link.parent,
			Name:   link.name,
			Child:  link.child,
			Fields: fields,
		})
		kg.Close()
		vg.Close()
	}
	return
}

func (s *shard) Stats() *shardStats {
	s.lock.RLock()
	replicates := make([]uint32, 0, len(s.nodes))
	for i := range s.nodes {
		replicates = append(replicates, i)
	}
	inoLimit := s.inoLimit
	routeVersion := s.routeVersion
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
		if cur >= s.startIno+proto.ShardFixedRange {
			return 0, apierrors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.inoCursor, cur, newId) {
			s.increaseInoUsed()
			return newId, nil
		}
	}
}

func (s *shard) checkInoMatchRange(ino uint64) bool {
	return s.startIno >= ino && s.startIno+proto.ShardFixedRange > ino
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
	atomic.AddUint64(&s.inoUsed, -1)
}

type shardKeys struct {
	shardId uint32
	spaceId uint64
}

func (s *shardKeys) encodeInoKey(ino uint64) []byte {
	key := make([]byte, shardDataPrefixSize(s.spaceId, s.shardId)+8)
	encodeShardDataPrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[len(key)-8:])
	return key
}

func (s *shardKeys) encodeLinkKey(ino uint64, name string) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	key := make([]byte, shardDataPrefixSize+8+len(infix)+len(name))
	encodeShardDataPrefix(s.spaceId, s.shardId, key)
	encodeIno(ino, key[shardDataPrefixSize:])
	copy(key[shardDataPrefixSize+8:], infix)
	copy(key[shardDataPrefixSize+8+len(infix):], util.StringsToBytes(name))
	return key
}

func (s *shardKeys) decodeLinkKey(key []byte) (ino uint64, name string) {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	ino = decodeIno(key[shardDataPrefixSize:])
	nameRaw := make([]byte, len(key[shardDataPrefixSize+8+len(infix):]))
	copy(nameRaw, key[shardDataPrefixSize+8+len(infix):])
	name = util.BytesToString(nameRaw)
	return
}

func (s *shardKeys) encodeLinkKeyPrefix(ino uint64) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.spaceId, s.shardId)
	keyPrefix := make([]byte, shardDataPrefixSize+8+len(infix))
	encodeShardDataPrefix(s.spaceId, s.shardId, keyPrefix)
	encodeIno(ino, keyPrefix[shardDataPrefixSize:])
	copy(keyPrefix[shardDataPrefixSize+8:], infix)
	return keyPrefix
}

func externalFieldsToInternalFields(external []*proto.Field) []field {
	ret := make([]field, len(external))
	for i := range external {
		ret[i] = field{
			name:  external[i].Name,
			value: external[i].Value,
		}
	}
	return ret
}

func internalFieldsToExternalFields(internal []field) []*proto.Field {
	ret := make([]*proto.Field, len(internal))
	for i := range internal {
		ret[i] = &proto.Field{
			Name:  internal[i].name,
			Value: internal[i].value,
		}
	}
	return ret
}
