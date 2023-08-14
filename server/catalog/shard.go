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
	"github.com/cubefs/inodedb/server/scalar"
	"github.com/cubefs/inodedb/server/store"
	"github.com/cubefs/inodedb/server/vector"
	"github.com/cubefs/inodedb/util"
)

const keyLocksNum = 1024

type shardMeta struct {
	id       uint32
	inoRange *proto.InoRange
}

func (s *shardMeta) Less(than btree.Item) bool {
	thanShard := than.(*shardMeta)
	return s.inoRange.StartIno < thanShard.inoRange.StartIno
}

func (s *shardMeta) Copy() btree.Item {
	return &(*s)
}

func newShard(sid uint64, shardId uint32, inoRange *proto.InoRange, replicates map[uint32]string, store *store.Store) *shard {
	shard := &shard{
		sid: sid,
		shardMeta: &shardMeta{
			id:       shardId,
			inoRange: inoRange,
		},
		replicates: replicates,
		store:      store,
	}
	return shard
}

type shardStat struct {
	inoUsed    uint64
	inoCursor  uint64
	inoRange   proto.InoRange
	replicates []uint32
}

type shard struct {
	sid         uint64
	inoCursor   uint64
	inoUsed     uint64
	replicates  map[uint32]string
	vectorIndex *vector.Index
	scalarIndex *scalar.Index
	store       *store.Store
	raftGroup   *RaftGroup

	keyLocks [keyLocksNum]sync.Mutex
	lock     sync.RWMutex

	// read only
	*shardMeta
}

func (s *shard) InsertItem(ctx context.Context, i *proto.Item) (uint64, error) {
	kvStore := s.store.KVStore()
	ino, err := s.nextIno()
	if err != nil {
		return 0, err
	}
	// TODO: transform into internal items
	data, err := (&item{
		ino:   ino,
		links: i.Links,
	}).Marshal()
	if err != nil {
		return 0, err
	}
	if err := kvStore.SetRaw(ctx, dataCF, s.encodeInoKey(ino), data, nil); err != nil {
		return 0, err
	}
	return ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, i *proto.Item) error {
	if !s.checkInoMatchRange(i.Ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	// TODO: transform into internal items
	data, err := (&item{
		ino:   i.Ino,
		links: i.Links,
	}).Marshal()
	if err != nil {
		return err
	}
	if err := kvStore.SetRaw(ctx, dataCF, s.encodeInoKey(i.Ino), data, nil); err != nil {
		return err
	}
	return nil
}

func (s *shard) DeleteItem(ctx context.Context, ino uint64) error {
	if !s.checkInoMatchRange(ino) {
		return apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	if err := kvStore.Delete(ctx, dataCF, s.encodeInoKey(ino), nil); err != nil {
		return err
	}
	return nil
}

func (s *shard) GetItem(ctx context.Context, ino uint64) (*proto.Item, error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	data, err := kvStore.GetRaw(ctx, dataCF, s.encodeInoKey(ino), nil)
	if err != nil {
		return nil, err
	}
	item := &item{}
	if err := item.Unmarshal(data); err != nil {
		return nil, err
	}
	// TODO: transform into external items
	return &proto.Item{Ino: item.ino, Links: item.links}, nil
}

func (s *shard) Link(ctx context.Context, l *proto.Link) error {
	if !s.checkInoMatchRange(l.Parent) {
		return apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	// TODO: transform into external link
	link := &link{
		parent: l.Parent,
		name:   l.Name,
		child:  l.Child,
	}
	linkData, err := link.Marshal()
	if err != nil {
		return errors.Info(err, "marshal link data failed")
	}

	pKey := s.encodeInoKey(link.parent)
	linkKey := s.encodeLinkKey(link.parent, link.name)
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

	kvStore := s.store.KVStore()
	pKey := s.encodeInoKey(ino)
	linkKey := s.encodeLinkKey(ino, name)
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
	return nil
}

func (s *shard) List(ctx context.Context, ino uint64, start string, num uint32) (ret []*proto.Link, err error) {
	if !s.checkInoMatchRange(ino) {
		return nil, apierrors.ErrInoMismatchShardRange
	}

	kvStore := s.store.KVStore()
	linkKeyPrefix := s.encodeLinkKeyPrefix(ino)
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
		_, link.name = s.decodeLinkKey(kg.Key())
		// TODO: transform to external link
		ret = append(ret, &proto.Link{
			Parent: link.parent,
			Name:   link.name,
			Child:  link.child,
		})
		kg.Close()
		vg.Close()
	}
	return
}

func (s *shard) Stats() *shardStat {
	s.lock.RLock()
	replicates := make([]uint32, 0, len(s.replicates))
	for i := range s.replicates {
		replicates = append(replicates, i)
	}
	inoRange := *s.inoRange
	s.lock.RUnlock()

	return &shardStat{
		inoUsed:    atomic.LoadUint64(&s.inoUsed),
		inoCursor:  atomic.LoadUint64(&s.inoCursor),
		inoRange:   inoRange,
		replicates: replicates,
	}
}

func (s *shard) Start() {
	s.raftGroup.Start()
}

func (s *shard) nextIno() (uint64, error) {
	if s.inoUsed > s.inoRange.InoLimit {
		return 0, apierrors.ErrInodeLimitExceed
	}
	for {
		cur := atomic.LoadUint64(&s.inoCursor)
		if cur >= s.inoRange.EndIno {
			return 0, apierrors.ErrInoOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&s.inoCursor, cur, newId) {
			atomic.AddUint64(&s.inoUsed, 1)
			return newId, nil
		}
	}
}

func (s *shard) checkInoMatchRange(ino uint64) bool {
	return s.inoRange.StartIno >= ino && s.inoRange.EndIno > ino
}

func (s *shard) encodeInoKey(ino uint64) []byte {
	key := make([]byte, shardDataPrefixSize(s.sid, s.id)+8)
	encodeShardDataPrefix(s.sid, s.id, key)
	encodeIno(ino, key[len(key)-8:])
	return key
}

func (s *shard) encodeLinkKey(ino uint64, name string) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.sid, s.id)
	key := make([]byte, shardDataPrefixSize+8+len(infix)+len(name))
	encodeShardDataPrefix(s.sid, s.id, key)
	encodeIno(ino, key[shardDataPrefixSize:])
	copy(key[shardDataPrefixSize+8:], infix)
	copy(key[shardDataPrefixSize+8+len(infix):], util.StringsToBytes(name))
	return key
}

func (s *shard) decodeLinkKey(key []byte) (ino uint64, name string) {
	shardDataPrefixSize := shardDataPrefixSize(s.sid, s.id)
	ino = decodeIno(key[shardDataPrefixSize:])
	nameRaw := make([]byte, len(key[shardDataPrefixSize+8+len(infix):]))
	copy(nameRaw, key[shardDataPrefixSize+8+len(infix):])
	name = util.BytesToString(nameRaw)
	return
}

func (s *shard) encodeLinkKeyPrefix(ino uint64) []byte {
	shardDataPrefixSize := shardDataPrefixSize(s.sid, s.id)
	keyPrefix := make([]byte, shardDataPrefixSize+8+len(infix))
	encodeShardDataPrefix(s.sid, s.id, keyPrefix)
	encodeIno(ino, keyPrefix[shardDataPrefixSize:])
	copy(keyPrefix[shardDataPrefixSize+8:], infix)
	return keyPrefix
}

func (s *shard) getKeyLock(key []byte) sync.Mutex {
	crc32 := crc32.NewIEEE()
	crc32.Write(key)
	idx := crc32.Sum32() % keyLocksNum
	return s.keyLocks[idx]
}
