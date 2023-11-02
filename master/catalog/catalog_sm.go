package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/proto"
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
	RaftOpDeleteSpace
)

func (c *catalog) Apply(ctx context.Context, op raft.Op, data []byte) error {
	switch op {
	case RaftOpCreateSpace:
		return c.applyCreateSpace(ctx, data)
	default:
		return errors.New(fmt.Sprintf("unsupported operation type: %d", op))
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

	space := newSpace(spaceInfo)
	c.spaces.PutNoLock(space)
	c.creatingSpaces.Store(spaceInfo.Name, nil)
	return nil
}

func (c *catalog) applyDeleteSpace(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)

	args := &deleteSpaceArgs{}
	if err := json.Unmarshal(data, args); err != nil {
		return errors.Info(err, "json unmarshal data failed")
	}

	spaceLock := c.spaces.GetLock(args.Sid)
	spaceLock.Lock()
	defer spaceLock.Unlock()

	if c.spaces.GetNoLock(args.Sid) != nil {
		span.Warnf("space[%s] already delete", args.Sid)
		return nil
	}

	routeItem := &routeItemInfo{
		RouteVersion: c.routeMgr.GenRouteVersion(ctx, 1),
		Type:         proto.CatalogChangeType_DeleteSpace,
		ItemDetail:   &routeItemSpaceDelete{Sid: args.Sid},
	}
	if err := c.storage.DeleteSpace(ctx, args.Sid, routeItem); err != nil {
		return err
	}

	c.spaces.Delete(args.Sid)
	c.routeMgr.InsertRouteItems(ctx, []*routeItemInfo{routeItem})
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
	if err := c.storage.UpsertSpaceShardsAndRouteItems(ctx, info, args.ShardInfos, nil); err != nil {
		info.Status = SpaceStatusInit
		return err
	}
	for i := range args.ShardInfos {
		space.PutShard(newShard(args.ShardInfos[i]))
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
	info.Epoch = args.SpaceEpoch
	routeItems := []*routeItemInfo{{
		RouteVersion: c.routeMgr.GenRouteVersion(ctx, 1),
		Type:         proto.CatalogChangeType_AddSpace,
		ItemDetail:   &routeItemSpaceAdd{Sid: args.Sid},
	}}
	routeItems = append(routeItems, c.genShardRouteItems(ctx, args.Sid, args.ShardInfos)...)
	if err := c.storage.UpsertSpaceShardsAndRouteItems(ctx, info, args.ShardInfos, routeItems); err != nil {
		info.Status = SpaceStatusUpdateRoute
		info.Epoch = 0
		return err
	}

	for i := range args.ShardInfos {
		space.PutShard(newShard(args.ShardInfos[i]))
	}
	c.routeMgr.InsertRouteItems(ctx, routeItems)
	c.creatingSpaces.Delete(info.Name)

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
		if reportInfo.Shard.Epoch < info.Epoch && !isReplicateMember(reportInfo.NodeId, info.Replicates) {
			ret.tasks = append(ret.tasks, &proto.ShardTask{
				Type:      proto.ShardTaskType_ClearShard,
				SpaceName: space.GetInfo().Name,
				ShardId:   reportInfo.Shard.Id,
				Epoch:     reportInfo.Shard.Epoch,
			})
			shard.lock.Unlock()
			continue
		}
		shard.UpdateReportInfoNoLock(reportInfo.Shard)
		shard.lock.Unlock()

		spaceCurrentShardId := space.GetCurrentShardId()
		if (shard.id >= spaceCurrentShardId-c.cfg.ExpandShardsNumPerSpace && shard.id <= spaceCurrentShardId) &&
			float64(reportInfo.Shard.InoUsed)/float64(reportInfo.Shard.InoLimit) >= defaultInoUsedThreshold {
			ret.maybeExpandingSpaces = append(ret.maybeExpandingSpaces, space)
		}

		// TODO: shard report info may need to be saved after raft flush or something
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
	if err := c.storage.UpsertSpaceShardsAndRouteItems(ctx, info, args.ShardInfos, nil); err != nil {
		info.ExpandStatus = SpaceExpandStatusNone
		return err
	}
	space.expandingShards = make([]*shard, len(args.ShardInfos))
	for i := range args.ShardInfos {
		space.expandingShards[i] = newShard(args.ShardInfos[i])
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
	routeItems := c.genShardRouteItems(ctx, args.Sid, args.ShardInfos)
	if err := c.storage.UpsertSpaceShardsAndRouteItems(ctx, info, args.ShardInfos, routeItems); err != nil {
		info.ExpandStatus = SpaceExpandStatusUpdateRoute
		return err
	}

	for i := range args.ShardInfos {
		space.PutShard(newShard(args.ShardInfos[i]))
	}
	space.expandingShards = nil
	c.routeMgr.InsertRouteItems(ctx, routeItems)

	return nil
}

func (c *catalog) genShardRouteItems(ctx context.Context, sid uint64, shardInfos []*shardInfo) []*routeItemInfo {
	routeItems := make([]*routeItemInfo, len(shardInfos))
	newRouteVersion := c.routeMgr.GenRouteVersion(ctx, uint64(len(shardInfos)))
	baseRouteVersion := newRouteVersion - uint64(len(shardInfos)) + 1

	for i := range shardInfos {
		routeItems[i] = &routeItemInfo{
			RouteVersion: baseRouteVersion + uint64(i),
			Type:         proto.CatalogChangeType_AddShard,
			ItemDetail: &routeItemShardAdd{
				Sid:     sid,
				ShardId: shardInfos[i].ShardId,
			},
		}
	}

	return routeItems
}

func isReplicateMember(target uint32, replicates []uint32) bool {
	for _, replicate := range replicates {
		if replicate == target {
			return true
		}
	}
	return false
}
