package IDGenter

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/inodedb/common/kvstore"
)

var cf = kvstore.CF("id")

type storage struct {
	kvStore kvstore.Store
}

func (s *storage) Load(ctx context.Context) (map[string]uint64, error) {
	lr := s.kvStore.List(ctx, cf, nil, nil, nil)
	defer lr.Close()

	ret := make(map[string]uint64)
	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			break
		}

		ret[decodeName(kg.Key())] = decodeValue(vg.Value())
		kg.Close()
		vg.Close()
	}

	return ret, nil
}

func (s *storage) Put(ctx context.Context, name string, commit uint64) error {
	key := encodeName(name)
	value := encodeValue(commit)
	err := s.kvStore.SetRaw(ctx, cf, key, value, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *storage) Get(ctx context.Context, name string) (uint64, error) {
	key := encodeName(name)
	v, err := s.kvStore.Get(ctx, cf, key, nil)
	if err != nil {
		return 0, err
	}

	current := decodeValue(v.Value())
	v.Close()
	return current, nil
}

func encodeName(name string) []byte {
	return []byte(name)
}

func decodeName(raw []byte) string {
	return string(raw)
}

func encodeValue(commit uint64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, commit)
	return v
}

func decodeValue(raw []byte) uint64 {
	return binary.BigEndian.Uint64(raw)
}
