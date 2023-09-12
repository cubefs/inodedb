package catalog

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/inodedb/master/store"

	"github.com/cubefs/inodedb/common/kvstore"
)

const CF = "catalog"

var (
	catalogKeyPrefix = []byte("c")
	routeKeyPrefix   = []byte("r")
	shardKeyPrefix   = []byte("s")
	keyInfix         = []byte("/")
)

func newStorage(kvStore *store.Store) *storage {
	return &storage{
		kvStore:       kvStore.KVStore(),
		keysGenerator: &keysGenerator{},
	}
}

type storage struct {
	kvStore       kvstore.Store
	keysGenerator *keysGenerator
}

func (s *storage) CreateSpace(ctx context.Context, info *spaceInfo) error {
	data, err := info.Marshal()
	if err != nil {
		return err
	}

	return s.kvStore.SetRaw(ctx, CF, s.keysGenerator.encodeSpaceKey(info.Sid), data, nil)
}

func (s *storage) ListSpaces(ctx context.Context) (ret []*spaceInfo, err error) {
	lr := s.kvStore.List(ctx, CF, s.keysGenerator.encodeSpaceKeyPrefix(), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}

		if kg == nil || vg == nil {
			return ret, nil
		}

		spaceInfo := &spaceInfo{}
		if err = spaceInfo.Unmarshal(vg.Value()); err != nil {
			kg.Close()
			vg.Close()
			return nil, err
		}

		ret = append(ret, spaceInfo)
		kg.Close()
		vg.Close()
	}
}

func (s *storage) DeleteSpace(ctx context.Context, sid uint64, info *routeItemInfo) error {
	data, err := info.Marshal()
	if err != nil {
		return err
	}

	batch := s.kvStore.NewWriteBatch()
	defer batch.Close()

	batch.Delete(CF, s.keysGenerator.encodeSpaceKey(sid))
	batch.Put(CF, s.keysGenerator.encodeRouteKey(info.RouteVersion), data)
	return s.kvStore.Write(ctx, batch, nil)
}

func (s *storage) UpsertSpaceShardsAndRouteItems(ctx context.Context, info *spaceInfo, shards []*shardInfo, routeItems []*routeItemInfo) error {
	batch := s.kvStore.NewWriteBatch()
	defer batch.Close()

	data, err := info.Marshal()
	if err != nil {
		return err
	}
	batch.Put(CF, s.keysGenerator.encodeSpaceKey(info.Sid), data)

	for i := range shards {
		shardData, err := shards[i].Marshal()
		if err != nil {
			return err
		}
		batch.Put(CF, s.keysGenerator.encodeShardKey(info.Sid, shards[i].ShardId), shardData)
	}

	for i := range routeItems {
		routeItemData, err := routeItems[i].Marshal()
		if err != nil {
			return err
		}
		batch.Put(CF, s.keysGenerator.encodeRouteKey(routeItems[i].RouteVersion), routeItemData)
	}

	return s.kvStore.Write(ctx, batch, nil)
}

func (s *storage) ListShards(ctx context.Context, sid uint64) (ret []*shardInfo, err error) {
	lr := s.kvStore.List(ctx, CF, s.keysGenerator.encodeShardKeyPrefix(sid), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			return ret, nil
		}

		shardInfo := &shardInfo{}
		if err = shardInfo.Unmarshal(vg.Value()); err != nil {
			kg.Close()
			vg.Close()
			return nil, err
		}

		ret = append(ret, shardInfo)
		kg.Close()
		vg.Close()
	}
}

func (s *storage) GetFirstRouteItem(ctx context.Context) (*routeItemInfo, error) {
	lr := s.kvStore.List(ctx, CF, s.keysGenerator.encodeRouteKeyPrefix(), nil, nil)
	defer lr.Close()

	kg, vg, err := lr.ReadNext()
	if err != nil {
		return nil, err
	}
	if kg == nil || vg == nil {
		return nil, nil
	}

	ret := &routeItemInfo{}
	err = ret.Unmarshal(vg.Value())
	kg.Close()
	vg.Close()
	return ret, err
}

func (s *storage) ListRouteItems(ctx context.Context) (ret []*routeItemInfo, err error) {
	lr := s.kvStore.List(ctx, CF, s.keysGenerator.encodeRouteKeyPrefix(), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			return ret, nil
		}

		item := &routeItemInfo{}
		if err = item.Unmarshal(vg.Value()); err != nil {
			return nil, err
		}

		ret = append(ret, item)
		kg.Close()
		vg.Close()
	}
}

func (s *storage) DeleteOldestRouteItems(ctx context.Context, before uint64) error {
	batch := s.kvStore.NewWriteBatch()
	defer batch.Close()

	batch.DeleteRange(CF, s.keysGenerator.encodeRouteKey(0), s.keysGenerator.encodeRouteKey(before))
	return s.kvStore.Write(ctx, batch, nil)
}

type keysGenerator struct{}

func (k *keysGenerator) encodeSpaceKey(sid uint64) []byte {
	ret := make([]byte, len(catalogKeyPrefix)+len(keyInfix)+8)
	copy(ret, catalogKeyPrefix)
	copy(ret[len(catalogKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint64(ret[len(ret)-8:], sid)
	return ret
}

func (k *keysGenerator) encodeShardKey(sid uint64, shardId uint32) []byte {
	ret := make([]byte, len(shardKeyPrefix)+len(keyInfix)+8+len(keyInfix)+4)
	copy(ret, shardKeyPrefix)
	copy(ret[len(shardKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint64(ret[len(shardKeyPrefix)+len(keyInfix):], sid)
	copy(ret[len(ret)-4-len(keyInfix):], keyInfix)
	binary.BigEndian.PutUint32(ret[len(ret)-4:], shardId)
	return ret
}

func (k *keysGenerator) encodeRouteKey(ver uint64) []byte {
	ret := make([]byte, len(routeKeyPrefix)+len(keyInfix)+8)
	copy(ret, routeKeyPrefix)
	copy(ret[len(routeKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint64(ret[len(ret)-8:], ver)
	return ret
}

func (k *keysGenerator) encodeSpaceKeyPrefix() []byte {
	ret := make([]byte, len(catalogKeyPrefix)+len(keyInfix))
	copy(ret, catalogKeyPrefix)
	copy(ret[len(catalogKeyPrefix):], keyInfix)
	return ret
}

func (k *keysGenerator) encodeShardKeyPrefix(sid uint64) []byte {
	ret := make([]byte, len(shardKeyPrefix)+len(keyInfix)+8+len(keyInfix))
	copy(ret, shardKeyPrefix)
	copy(ret[len(shardKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint64(ret[len(shardKeyPrefix)+len(keyInfix):], sid)
	copy(ret[len(ret)-len(keyInfix):], keyInfix)
	return ret
}

func (k *keysGenerator) encodeRouteKeyPrefix() []byte {
	ret := make([]byte, len(routeKeyPrefix)+len(keyInfix))
	copy(ret, routeKeyPrefix)
	copy(ret[len(routeKeyPrefix):], keyInfix)
	return ret
}
