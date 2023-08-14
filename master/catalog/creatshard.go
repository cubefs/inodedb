package catalog

import (
	"context"
	"encoding/json"
	"sync"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

type CreateShardTask struct {
	ShardInfo *ShardInfo
	ShardID   uint32
}

func (c *CreateShardTask) Encode() (data []byte, err error) {
	data, err = json.Marshal(c)
	return
}

func (c *CreateShardTask) Decode(raw []byte) error {
	return json.Unmarshal(raw, c)
}

func (s *space) addShard(ctx context.Context, shardID uint32) error {
	span := trace.SpanFromContext(ctx)
	old := s.allShards.Get(shardID)
	if old != nil {
		span.Warnf("shard[%v] has added", shardID)
		return errors.ErrCreateShardAlreadyExist
	}

	// 1. alloc nodes
	args := &cluster.AllocArgs{Count: 3}
	alloc, err := s.cluster.Alloc(ctx, args)
	if err != nil {
		span.Errorf("alloc shard server node failed, err: %v", err)
		return err
	}

	// 2. generate add shard task
	info := &ShardInfo{
		ShardId:    shardID,
		InoLimit:   s.inoLimit,
		Replicates: alloc,
	}
	task := &CreateShardTask{
		ShardID:   shardID,
		ShardInfo: info,
	}

	// todo 3. raft propose and persistent
	_, err = task.Encode()
	if err != nil {
		span.Errorf("encode CreateShardTask failed, err: %v", err)
		return err
	}
	err = s.doAddShard(ctx, task)
	if err != nil {
		span.Errorf("doAddShard task[%v] failed, err: %s", task, err)
		return err
	}
	span.Infof("add shard[%v] success", info)
	return nil
}

func (s *space) doAddShard(ctx context.Context, task *CreateShardTask) error {
	span := trace.SpanFromContext(ctx)
	// 1. alloc shard from node
	err := s.reqAddShard(ctx, task)
	if err != nil {
		span.Errorf("add shard failed, err: %v", err)
		return err
	}

	// 2. add router
	rps := make(map[uint32]string, len(task.ShardInfo.Replicates))
	for _, node := range task.ShardInfo.Replicates {
		rps[node.Id] = node.Addr
	}
	item := &ChangeItem{Type: proto.CatalogChangeType_AddShard, Item: &anypb.Any{}}
	err = item.Item.MarshalFrom(&proto.CatalogChangeShardAdd{
		ShardId:    task.ShardID,
		SpaceName:  s.info.Name,
		InoLimit:   s.inoLimit,
		Replicates: rps,
	})
	if err != nil {
		return err
	}
	err = s.routerMgr.AddRouter(ctx, item)
	if err != nil {
		return err
	}
	// todo 3. after alloc update node load and router
	task.ShardInfo.RouteVersion = item.RouteVersion
	s.allShards.Put(&shard{
		shardID: task.ShardID,
		info:    task.ShardInfo,
	})
	return nil
}

func (s *space) reqAddShard(ctx context.Context, task *CreateShardTask) error {
	span := trace.SpanFromContextSafe(ctx)
	var wg sync.WaitGroup
	replicates := make(map[uint32]string, len(task.ShardInfo.Replicates))
	for _, replica := range task.ShardInfo.Replicates {
		replicates[replica.Id] = replica.Addr
	}
	errs := make([]error, len(task.ShardInfo.Replicates))
	for idx, replica := range task.ShardInfo.Replicates {
		wg.Add(1)
		go func(i int, r *cluster.NodeInfo) {
			_, err := s.serverClient.AddShard(ctx, r.Addr, &proto.AddShardRequest{
				ShardId:    task.ShardID,
				Sid:        &s.spaceID,
				SpaceName:  s.info.Name,
				InoLimit:   s.inoLimit,
				Replicates: replicates,
			})
			if err != nil {
				span.Errorf("add shard[%d] failed, node: %s, err: %v", task.ShardID, r.Addr, err)
				errs[i] = err
			}
			wg.Done()
		}(idx, replica)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
