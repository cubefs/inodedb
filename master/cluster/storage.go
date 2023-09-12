package cluster

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/inodedb/common/kvstore"
)

const CF = "node"

var (
	nodeKeyPrefix = []byte("n")
	keyInfix      = []byte("/")
)

type storage struct {
	kvStore kvstore.Store
}

func (s *storage) Load(ctx context.Context) ([]*nodeInfo, error) {
	lr := s.kvStore.List(ctx, CF, nil, nil, nil)
	defer lr.Close()

	var res []*nodeInfo
	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			break
		}
		newNode := &nodeInfo{}
		err = newNode.Unmarshal(vg.Value())
		if err != nil {
			kg.Close()
			vg.Close()
			return nil, err
		}
		res = append(res, newNode)
		kg.Close()
		vg.Close()
	}

	return res, nil
}

func (s *storage) Put(ctx context.Context, info *nodeInfo) error {
	key := encodeNodeKey(info.Id)
	marshal, err := info.Marshal()
	if err != nil {
		return err
	}
	return s.kvStore.SetRaw(ctx, CF, key, marshal, nil)
}

func (s *storage) Get(ctx context.Context, nodeId uint32) (*nodeInfo, error) {
	key := encodeNodeKey(nodeId)
	v, err := s.kvStore.Get(ctx, CF, key, nil)
	if err != nil {
		return nil, err
	}

	res := &nodeInfo{}
	err = res.Unmarshal(v.Value())
	v.Close()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *storage) Delete(ctx context.Context, nodeId uint32) error {
	key := encodeNodeKey(nodeId)
	return s.kvStore.Delete(ctx, CF, key, nil)
}

func encodeNodeKey(nodeId uint32) []byte {
	ret := make([]byte, len(nodeKeyPrefix)+len(keyInfix)+4)
	ret = append(ret, nodeKeyPrefix...)
	ret = append(ret, keyInfix...)
	binary.BigEndian.PutUint32(ret[len(ret)-4:], nodeId)
	return ret
}
