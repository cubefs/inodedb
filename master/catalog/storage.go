package catalog

import (
	"context"

	"github.com/cubefs/inodedb/common/kvstore"
)

var cf = "catalog"

func newStorage() *storage {
	return &storage{}
}

type storage struct {
	kvStore       kvstore.Store
	keysGenerator *keysGenerator
}

func (s *storage) CreateSpace(ctx context.Context, info *spaceInfo) error {
	return nil
}

func (s *storage) UpsertSpaceShards(ctx context.Context, info *spaceInfo, shards []*shardInfo) error {
	return nil
}

type keysGenerator struct{}

func (k *keysGenerator) encodeTaskKey() []byte {
	return nil
}

func (k *keysGenerator) encodeSpaceKey() []byte {
	return nil
}

func (k *keysGenerator) encodeShardKey() []byte {
	return nil
}
