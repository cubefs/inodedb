package cluster

import (
	"context"
	"encoding/binary"

	"github.com/cubefs/inodedb/common/kvstore"
)

const CF = "node"

var (
	nodeKeyPrefix = []byte("n")
	diskKeyPrefix = []byte("d")
	keyInfix      = []byte("/")
)

type storage struct {
	kvStore kvstore.Store
}

func (s *storage) Load(ctx context.Context) ([]*nodeInfo, error) {
	lr := s.kvStore.List(ctx, CF, encodeNodeKeyPrefix(), nil, nil)
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

func (s *storage) LoadDisk(ctx context.Context) ([]*diskInfo, error) {
	lr := s.kvStore.List(ctx, CF, encodeDiskKeyPrefix(), nil, nil)
	defer lr.Close()

	res := make([]*diskInfo, 0)
	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return nil, err
		}
		if kg == nil || vg == nil {
			break
		}
		newDisk := &diskInfo{}
		err = newDisk.decode(vg.Value())
		if err != nil {
			kg.Close()
			vg.Close()
			return nil, err
		}
		res = append(res, newDisk)
		kg.Close()
		vg.Close()
	}

	return res, nil
}

func (s *storage) Put(ctx context.Context, info *nodeInfo) error {
	key := encodeNodeKey(info.ID)
	marshal, err := info.Marshal()
	if err != nil {
		return err
	}
	return s.kvStore.SetRaw(ctx, CF, key, marshal, nil)
}

func (s *storage) PutDisk(ctx context.Context, disk *diskInfo) error {
	key := encodeDiskKey(disk.DiskID)
	marshal, err := disk.encode()
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

func (s *storage) DeleteDisk(ctx context.Context, nodeId uint32) error {
	key := encodeDiskKey(nodeId)
	return s.kvStore.Delete(ctx, CF, key, nil)
}

func encodeNodeKey(nodeId uint32) []byte {
	ret := make([]byte, len(nodeKeyPrefix)+len(keyInfix)+4)
	copy(ret, nodeKeyPrefix)
	copy(ret[len(nodeKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint32(ret[len(ret)-4:], nodeId)
	return ret
}

func encodeDiskKey(diskID uint32) []byte {
	ret := make([]byte, len(diskKeyPrefix)+len(keyInfix)+4)
	copy(ret, diskKeyPrefix)
	copy(ret[len(diskKeyPrefix):], keyInfix)
	binary.BigEndian.PutUint32(ret[len(ret)-4:], diskID)
	return ret
}

func encodeNodeKeyPrefix() []byte {
	ret := make([]byte, len(nodeKeyPrefix)+len(keyInfix))
	copy(ret, diskKeyPrefix)
	copy(ret[len(diskKeyPrefix):], keyInfix)
	return ret
}

func encodeDiskKeyPrefix() []byte {
	ret := make([]byte, len(diskKeyPrefix)+len(keyInfix))
	copy(ret, diskKeyPrefix)
	copy(ret[len(diskKeyPrefix):], keyInfix)
	return ret
}
