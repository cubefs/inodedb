package catalog

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/proto"
)

type catalogTask Catalog

func (c *catalogTask) executeShardTask(ctx context.Context, task proto.ShardTask) error {
	span := trace.SpanFromContext(ctx)

	disk, err := (*Catalog)(c).getDisk(task.DiskID)
	if err != nil {
		return errors.Info(err, "get disk failed", task.Sid)
	}
	shard, err := disk.GetShard(task.Sid, task.ShardID)
	if err != nil {
		return errors.Info(err, "get shard failed", task.ShardID)
	}

	switch task.Type {
	case proto.ShardTask_ClearShard:
		c.taskPool.Run(func() {
			if shard.GetEpoch() == task.Epoch {
				err := disk.DeleteShard(ctx, task.Sid, task.ShardID)
				if err != nil {
					span.Errorf("delete shard task[%+v] failed: %s", task, err)
				}
			}
		})
	case proto.ShardTask_Checkpoint:
		c.taskPool.Run(func() {
			if err := shard.Checkpoint(ctx); err != nil {
				span.Errorf("do shard checkpoint task[%+v] failed: %s", task, err)
			}
		})
	default:
	}
	return nil
}
