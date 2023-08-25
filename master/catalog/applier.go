package catalog

import (
	"context"
	"encoding/json"

	"github.com/cubefs/inodedb/proto"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/raft"
)

const (
	module = "catalog"
)

const (
	RaftOpCreateSpace raft.Op = iota + 1
	RaftOpInitSpaceShards
	RaftOpUpdateSpaceRoute
	RaftOpShardReport
	RaftOpExpandSpaceCreateShards
	RaftOpExpandSpaceUpdateRoute
)

func (c *catalog) Apply(ctx context.Context, op raft.Op, data []byte) error {
	switch op {
	case RaftOpCreateSpace:
		return c.applyCreateSpace(ctx, data)
	default:
		return errors.New("unsupported operation")
	}
}

func (c *catalog) LeaderChange(leader uint64, addr string) error {
	if leader != c.raftGroup.Stat().Id {
		c.taskMgr.Close()
		return nil
	}

	c.taskMgr.Start()

	// TODO: read index and load data

	return nil
}

func (c *catalog) applyCreateSpace(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	spaceInfo := &spaceInfo{}
	if err := spaceInfo.Unmarshal(data); err != nil {
		return errors.Info(err, "json unmarshal failed")
	}

	spaceLock := c.spaces.GetLockByName(spaceInfo.Name)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	if c.spaces.GetByNameNoLock(spaceInfo.Name) != nil {
		span.Warnf("space[%s] already created", spaceInfo.Name)
		return nil
	}

	if err := c.storage.CreateSpace(ctx, spaceInfo); err != nil {
		return err
	}

	space := &space{
		id:     spaceInfo.Sid,
		info:   spaceInfo,
		shards: newConcurrentShards(defaultSplitMapNum),
	}
	c.spaces.Put(space)
	return nil
}

func (c *catalog) applyInitSpaceShards(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	args := &createSpaceShardsArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return errors.Info(err, "json unmarshal data failed")
	}

	spaceLock := c.spaces.GetLock(args.Sid)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	space := c.spaces.GetNoLock(args.Sid)
	if !space.IsInit(false) {
		span.Warnf("space[%+v] initial shards already done", space.info)
		return nil
	}

	info := space.info
	info.Status = SpaceStatusUpdateRoute
	if err := c.storage.UpsertSpaceShards(ctx, info, args.ShardInfos); err != nil {
		info.Status = SpaceStatusInit
		return err
	}
	for i := range args.ShardInfos {
		space.PutShard(&shard{
			id:   args.ShardInfos[i].ShardId,
			info: args.ShardInfos[i],
		})
	}

	return nil
}

func (c *catalog) applyUpdateSpaceRoute(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	args := &updateSpaceRouteArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return errors.Info(err, "json unmarshal data failed")
	}

	spaceLock := c.spaces.GetLock(args.Sid)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	space := c.spaces.GetNoLock(args.Sid)
	if !space.IsUpdateRoute(false) {
		span.Warnf("space[%+v] update route already done", space.info)
		return nil
	}

	info := space.info
	info.Status = SpaceStatusNormal
	info.RouteVersion = args.SpaceRouteVersion

	if err := c.storage.UpsertSpaceShards(ctx, info, args.ShardInfos); err != nil {
		info.Status = SpaceStatusInit
		info.RouteVersion = 0
		return err
	}

	for i := range args.ShardInfos {
		space.PutShard(&shard{
			id:   args.ShardInfos[i].ShardId,
			info: args.ShardInfos[i],
		})
	}

	return nil
}

func (c *catalog) applyShardReport(ctx context.Context, data []byte) (ret *shardReportsResult, err error) {
	ret = &shardReportsResult{}

	args := &shardReportsArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return nil, errors.Info(err, "json unmarshal data failed")
	}

	for _, reportInfo := range args.infos {
		space := c.spaces.Get(reportInfo.Sid)
		shard := space.GetShard(reportInfo.Shard.Id)
		// shard may not find when space is expanding
		if shard == nil {
			continue
		}

		shard.lock.Lock()
		info := shard.info
		if reportInfo.Shard.RouteVersion < info.RouteVersion && isReplicateMember(reportInfo.Shard.Id, info.Replicates) {
			ret.tasks = append(ret.tasks, &proto.ShardTask{
				Type:    proto.ShardTaskType_ClearShard,
				Sid:     reportInfo.Sid,
				ShardId: reportInfo.Shard.Id,
			})
			shard.lock.Unlock()
			continue
		}
		shard.UpdateReportInfoNoLock(reportInfo.Shard)
		shard.lock.Unlock()

		if float64(reportInfo.Shard.InoUsed)/float64(reportInfo.Shard.InoLimit) <= defaultInoUsedThreshold {
			ret.maybeExpandingSpaces = append(ret.maybeExpandingSpaces, space)
		}
	}

	return
}

func (c *catalog) applyExpandSpaceShards(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	args := &createSpaceShardsArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return errors.Info(err, "json unmarshal data failed")
	}

	spaceLock := c.spaces.GetLock(args.Sid)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	space := c.spaces.GetNoLock(args.Sid)
	if !space.IsExpandUpdateNone(false) {
		span.Warnf("space[%+v] expand shards already done", space.info)
		return nil
	}

	info := space.info
	info.ExpandStatus = SpaceExpandStatusUpdateRoute
	if err := c.storage.UpsertSpaceShards(ctx, info, args.ShardInfos); err != nil {
		info.ExpandStatus = SpaceExpandStatusNone
		return err
	}
	space.expandingShards = make([]*shard, len(args.ShardInfos))
	for i := range args.ShardInfos {
		space.expandingShards[i] = &shard{
			id:   args.ShardInfos[i].ShardId,
			info: args.ShardInfos[i],
		}
	}

	return nil
}

func (c *catalog) applyExpandSpaceUpdateRoute(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	args := &createSpaceShardsArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return errors.Info(err, "json unmarshal data failed")
	}

	spaceLock := c.spaces.GetLock(args.Sid)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	space := c.spaces.GetNoLock(args.Sid)
	if !space.IsExpandUpdateRoute(false) {
		span.Warnf("space[%+v] expand update route already done", space.info)
		return nil
	}

	info := space.info
	info.ExpandStatus = SpaceExpandStatusNone
	if err := c.storage.UpsertSpaceShards(ctx, info, args.ShardInfos); err != nil {
		info.ExpandStatus = SpaceExpandStatusUpdateRoute
		return err
	}

	for i := range args.ShardInfos {
		space.PutShard(&shard{
			id:   args.ShardInfos[i].ShardId,
			info: args.ShardInfos[i],
		})
	}
	space.expandingShards = nil

	return nil
}

func isReplicateMember(target uint32, replicates []uint32) bool {
	for _, replicate := range replicates {
		if replicate == target {
			return true
		}
	}
	return false
}
