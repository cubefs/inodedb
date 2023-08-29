package cluster

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/inodedb/common/kvstore"
)

type storage struct {
	kvStore kvstore.Store
}

func (s *storage) Load(ctx context.Context) ([]*nodeInfo, error) {
	lr := s.kvStore.List(ctx, clusterCF, nil, nil, nil)
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
			return nil, err
		}
		res = append(res, newNode)
		kg.Close()
		vg.Close()
	}

	return res, nil
}

func (s *storage) Put(ctx context.Context, info *nodeInfo) error {
	key := encodeKey(info.Id)
	marshal, err := info.Marshal()
	if err != nil {
		return err
	}
	err = s.kvStore.SetRaw(ctx, clusterCF, key, marshal, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *storage) Get(ctx context.Context, nodeId uint32) (*nodeInfo, error) {
	key := encodeKey(nodeId)
	v, err := s.kvStore.Get(ctx, clusterCF, key, nil)
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
	key := encodeKey(nodeId)
	err := s.kvStore.Delete(ctx, clusterCF, key, nil)
	if err != nil {
		return err
	}
	return nil
}

func encodeKey(nodeId uint32) []byte {
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v, nodeId)
	return v
}
