package catalog

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/cubefs/inodedb/common/kvstore"
)

var (
	cf = kvstore.CF("catalog")

	catalogKeyPrefix = []byte("c")
	routeKeyPrefix   = []byte("r")
	shardKeyPrefix   = []byte("s")
	keyInfix         = []byte("/")
)

func newStorage(kvStore kvstore.Store) *storage {
	return &storage{
		kvStore:       kvStore,
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

	return s.kvStore.SetRaw(ctx, cf, s.keysGenerator.encodeSpaceKey(info.Sid), data, nil)
}

func (s *storage) ListSpaces(ctx context.Context) (ret []*spaceInfo, err error) {
	lr := s.kvStore.List(ctx, cf, s.keysGenerator.encodeSpaceKeyPrefix(), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			return
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

	batch.Delete(cf, s.keysGenerator.encodeSpaceKey(sid))
	batch.Put(cf, s.keysGenerator.encodeRouteKey(info.RouteVersion), data)
	return s.kvStore.Write(ctx, batch, nil)
}

func (s *storage) UpsertSpaceShardsAndRouteItems(ctx context.Context, info *spaceInfo, shards []*shardInfo, routeItems []*routeItemInfo) error {
	if len(routeItems) > 0 && len(shards) != len(routeItems) {
		return errors.New("route items and shards num mismatch")
	}

	batch := s.kvStore.NewWriteBatch()
	defer batch.Close()

	data, err := info.Marshal()
	if err != nil {
		return err
	}
	batch.Put(cf, s.keysGenerator.encodeSpaceKey(info.Sid), data)

	for i := range shards {
		shardData, err := shards[i].Marshal()
		if err != nil {
			return err
		}
		routeItemData, err := routeItems[i].Marshal()
		if err != nil {
			return err
		}

		batch.Put(cf, s.keysGenerator.encodeShardKey(info.Sid, shards[i].ShardId), shardData)
		batch.Put(cf, s.keysGenerator.encodeRouteKey(routeItems[i].RouteVersion), routeItemData)
	}

	return s.kvStore.Write(ctx, batch, nil)
}

func (s *storage) ListShards(ctx context.Context, sid uint64) (ret []*shardInfo, err error) {
	lr := s.kvStore.List(ctx, cf, s.keysGenerator.encodeShardKeyPrefix(sid), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			return
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
	lr := s.kvStore.List(ctx, cf, s.keysGenerator.encodeRouteKeyPrefix(), nil, nil)
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
	lr := s.kvStore.List(ctx, cf, s.keysGenerator.encodeRouteKeyPrefix(), nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			return
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

	batch.DeleteRange(cf, s.keysGenerator.encodeRouteKey(0), s.keysGenerator.encodeRouteKey(before))
	return s.kvStore.Write(ctx, batch, nil)
}

type keysGenerator struct{}

func (k *keysGenerator) encodeSpaceKey(sid uint64) []byte {
	ret := make([]byte, 0, len(catalogKeyPrefix)+len(keyInfix)+8)
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint64(ret[cap(ret)-8:], sid)
	return ret
}

func (k *keysGenerator) encodeShardKey(sid uint64, shardId uint32) []byte {
	ret := make([]byte, 0, len(catalogKeyPrefix)+len(keyInfix)+8+len(shardKeyPrefix)+len(keyInfix)+4)
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint64(ret[len(catalogKeyPrefix)+len(keyInfix):], sid)
	ret = append(ret, shardKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint32(ret[cap(ret)-4:], shardId)
	return ret
}

func (k *keysGenerator) encodeRouteKey(ver uint64) []byte {
	ret := make([]byte, 0, len(routeKeyPrefix)+len(keyInfix)+8)
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint64(ret[cap(ret)-8:], ver)
	return ret
}

func (k *keysGenerator) encodeSpaceKeyPrefix() []byte {
	ret := make([]byte, 0, len(catalogKeyPrefix)+len(keyInfix))
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	return ret
}

func (k *keysGenerator) encodeShardKeyPrefix(sid uint64) []byte {
	ret := make([]byte, 0, len(catalogKeyPrefix)+len(keyInfix)+8+len(shardKeyPrefix)+len(keyInfix))
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint64(ret[len(catalogKeyPrefix)+len(keyInfix):], sid)
	ret = append(ret, shardKeyPrefix...)
	ret = append(ret, keyInfix...)
	return ret
}

func (k *keysGenerator) encodeRouteKeyPrefix() []byte {
	ret := make([]byte, 0, len(routeKeyPrefix)+len(keyInfix))
	ret = append(ret, catalogKeyPrefix...)
	ret = append(ret, keyInfix...)
	return ret
}
