package catalog

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

type ShardInfo struct {
	ShardID proto.ShardID
	Epoch   uint64
	Leader  proto.DiskID
	Nodes   []proto.ShardNode
}

type shard struct {
	shardID uint32
	epoch   uint64
	tr      *transport

	info *ShardInfo
	sync.RWMutex
}

func (s *shard) InsertItem(ctx context.Context, req *proto.ShardInsertItemRequest) (uint64, error) {
	ret, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardInsertItem(ctx, req)
	})
	if err != nil {
		return 0, err
	}

	return ret.(*proto.ShardInsertItemResponse).Ino, nil
}

func (s *shard) UpdateItem(ctx context.Context, req *proto.ShardUpdateItemRequest) error {
	_, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardUpdateItem(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *shard) DeleteItem(ctx context.Context, req *proto.ShardDeleteItemRequest) error {
	_, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardDeleteItem(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *shard) GetItem(ctx context.Context, req *proto.ShardGetItemRequest) (proto.Item, error) {
	ret, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardGetItem(ctx, req)
	})
	if err != nil {
		return proto.Item{}, err
	}

	return ret.(*proto.ShardGetItemResponse).Item, nil
}

func (s *shard) Link(ctx context.Context, req *proto.ShardLinkRequest) error {
	_, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardLink(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *shard) Unlink(ctx context.Context, req *proto.ShardUnlinkRequest) error {
	_, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardUnlink(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *shard) List(ctx context.Context, req *proto.ShardListRequest) ([]proto.Link, error) {
	ret, err := s.doShardOperationRetry(ctx, func(sc ShardServerClient) (interface{}, error) {
		req.Header.DiskID = sc.ShardDiskID()
		return sc.ShardList(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	return ret.(*proto.ShardListResponse).Links, nil
}

func (s *shard) doShardOperationRetry(ctx context.Context, f func(sc ShardServerClient) (interface{}, error)) (interface{}, error) {
	scs, err := s.getShardClients(ctx)
	if err != nil && len(scs) == 0 {
		return nil, err
	}

	idx := 0
RETRY:
	ret, err := f(scs[idx])
	if err != nil {
		if idx == len(scs)-1 || err.Error() != errors.ErrNotLeader.Error() {
			return 0, err
		}
		idx++
		goto RETRY
	}

	if idx > 0 {
		s.updateLeader(scs[idx].ShardDiskID())
	}
	return ret, nil
}

func (s *shard) getShardClients(ctx context.Context) (ret []ShardServerClient, err error) {
	nodeIDs := s.getNodeIDs()
	for _, id := range nodeIDs {
		client, err := s.tr.GetClient(ctx, id)
		if err != nil {
			span := trace.SpanFromContext(ctx)
			span.Warnf("get shard server client failed: %s", err)
			return ret, err
		}
		ret = append(ret, client)
	}
	return
}

func (s *shard) getNodeIDs() (ret []proto.DiskID) {
	s.RLock()
	ret = make([]proto.DiskID, 0, len(s.info.Nodes))
	leader := s.info.Leader
	for i := range s.info.Nodes {
		if leader != 0 && s.info.Nodes[i].DiskID == leader {
			continue
		}
		ret = append(ret, s.info.Nodes[i].DiskID)
	}
	s.RUnlock()

	return
}

func (s *shard) updateLeader(leader proto.DiskID) {
	s.Lock()
	s.info.Leader = leader
	s.Unlock()
}
