package catalog

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/proto"
)

type catalogTask Catalog

func (c *catalogTask) executeShardTask(ctx context.Context, task *proto.ShardTask) error {
	span := trace.SpanFromContext(ctx)

	space, err := (*Catalog)(c).GetSpace(ctx, task.SpaceName)
	if err != nil {
		return errors.Info(err, "get space failed", task.SpaceName)
	}
	shard, err := space.GetShard(ctx, task.ShardId)
	if err != nil {
		return errors.Info(err, "get shard failed", task.ShardId)
	}

	switch task.Type {
	case proto.ShardTaskType_ClearShard:
		c.taskPool.Run(func() {
			if shard.GetEpoch() == task.Epoch {
				err := space.DeleteShard(ctx, task.ShardId)
				if err != nil {
					span.Errorf("delete shard task[%+v] failed: %s", task, err)
				}
			}
		})
	case proto.ShardTaskType_Checkpoint:
		c.taskPool.Run(func() {
			if err := shard.Checkpoint(ctx); err != nil {
				span.Errorf("do shard checkpoint task[%+v] failed: %s", task, err)
			}
		})
	default:
	}
	return nil
}
