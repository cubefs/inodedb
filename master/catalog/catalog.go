package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/raft"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/proto"
)

const (
	spaceIdName       = "space"
	shardIdNamePrefix = "%d-shard"
	epochIdName       = "epoch"
)

const (
	MaxDesiredShardsNum = 1024

	defaultSplitMapNum             = 16
	defaultTaskPoolNum             = 16
	defaultShardReplicateNum       = 3
	defaultInoLimitPerShard        = 4 << 20
	defaultInoUsedThreshold        = 0.4
	defaultExpandShardsNumPerSpace = 32
)

type Catalog interface {
	CreateSpace(ctx context.Context, SpaceName string, SpaceType proto.SpaceType, DesiredShards uint32, FixedFields []*proto.FieldMeta) error
	DeleteSpace(ctx context.Context, sid uint64) error
	DeleteSpaceByName(ctx context.Context, name string) error
	GetSpace(ctx context.Context, sid uint64) (*proto.SpaceMeta, error)
	GetSpaceByName(ctx context.Context, name string) (*proto.SpaceMeta, error)
	Report(ctx context.Context, nodeId uint32, infos []*proto.ShardReport) ([]*proto.ShardTask, error)
	GetCatalogChanges(ctx context.Context, routerVersion uint64, nodeId uint32) ([]*proto.CatalogChangeItem, error)
	Close()
}

type Config struct {
	ShardReplicateNum       int    `json:"shard_replicate_num"`
	InoLimitPerShard        uint64 `json:"ino_limit_per_shard"`
	ExpandShardsNumPerSpace uint32 `json:"expand_shards_num_per_space"`
	IdGenerator             idgenerator.IDGenerator
}

type (
	catalog struct {
		spaces         *concurrentSpaces
		creatingSpaces sync.Map

		idGenerator idgenerator.IDGenerator
		raftGroup   raft.Group
		storage     *storage
		taskMgr     *taskMgr
		routeMgr    *routeMgr
		cluster     cluster.Cluster
		cfg         Config

		done chan struct{}
		lock sync.RWMutex
	}

	createSpaceShardsArgs struct {
		Sid        uint64
		ShardInfos []*shardInfo
	}
	updateSpaceRouteArgs struct {
		createSpaceShardsArgs
		SpaceEpoch uint64
	}
	deleteSpaceArgs struct {
		Sid uint64
	}
	shardReportsArgs struct {
		nodeId uint32
		infos  []*proto.ShardReport
	}
	shardReportsResult struct {
		tasks                []*proto.ShardTask
		maybeExpandingSpaces []*space
	}
)

func NewCatalog(ctx context.Context, cfg *Config) Catalog {
	c := &catalog{
		spaces:      newConcurrentSpaces(defaultSplitMapNum),
		idGenerator: cfg.IdGenerator,
		storage:     newStorage(),
		taskMgr:     newTaskMgr(defaultTaskPoolNum),
		done:        make(chan struct{}),
	}

	c.taskMgr.Register(taskTypeCreateSpace, c.createSpaceTask)
	c.taskMgr.Register(taskTypeExpandSpace, c.expandSpaceTask)
	return c
}

func (c *catalog) CreateSpace(
	ctx context.Context,
	spaceName string,
	spaceType proto.SpaceType,
	desiredShards uint32,
	fixedFields []*proto.FieldMeta,
) error {
	if desiredShards > MaxDesiredShardsNum {
		return apierrors.ErrExceedMaxDesiredShardNum
	}
	if c.spaces.GetByName(spaceName) != nil {
		return nil
	}
	if _, ok := c.creatingSpaces.Load(spaceName); ok {
		return apierrors.ErrSpaceCreating
	}

	_, sid, err := c.idGenerator.Alloc(ctx, spaceIdName, 1)
	if err != nil {
		return errors.Info(err, "alloc sid failed")
	}

	spaceInfo := &spaceInfo{
		Sid:             sid,
		Name:            spaceName,
		Type:            spaceType,
		Status:          SpaceStatusInit,
		DesiredShardNum: desiredShards,
		CurrentShardId:  desiredShards,
		FixedFields:     protoFieldMetasToInternalFieldMetas(fixedFields),
	}

	data, err := spaceInfo.Marshal()
	if err != nil {
		return errors.Info(err, "marshal create space argument failed")
	}

	if _, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpCreateSpace,
		Data:       data,
		WithResult: false,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke raft
	if err := c.applyCreateSpace(ctx, data); err != nil {
		return err
	}

	// start create space progress, like add shard etc.
	if c.spaces.Get(spaceInfo.Sid) != nil {
		c.taskMgr.Send(ctx, &task{
			sid: spaceInfo.Sid,
			typ: taskTypeCreateSpace,
		})
	}

	return nil
}

func (c *catalog) DeleteSpace(ctx context.Context, sid uint64) error {
	space := c.spaces.Get(sid)
	if space == nil {
		return apierrors.ErrSpaceNotExist
	}

	data, err := json.Marshal(&deleteSpaceArgs{Sid: sid})
	if err != nil {
		return errors.Info(err, "marshal delete space argument failed")
	}
	if _, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module: module,
		Op:     RaftOpDeleteSpace,
		Data:   data,
	}); err != nil {
		return err
	}

	// TODO: remove this function call after invoke raft
	return c.applyDeleteSpace(ctx, data)
}

func (c *catalog) DeleteSpaceByName(ctx context.Context, name string) error {
	space := c.spaces.GetByName(name)
	return c.DeleteSpace(ctx, space.id)
}

func (c *catalog) GetSpace(ctx context.Context, sid uint64) (*proto.SpaceMeta, error) {
	space := c.spaces.Get(sid)
	if space == nil {
		return nil, apierrors.ErrSpaceNotExist
	}

	spaceInfo := space.GetInfo()
	meta := &proto.SpaceMeta{
		Sid:         spaceInfo.Sid,
		Name:        spaceInfo.Name,
		Type:        spaceInfo.Type,
		FixedFields: internalFieldMetasToProtoFieldMetas(spaceInfo.FixedFields),
	}

	shards := space.GetAllShards()
	meta.Shards = make([]*proto.Shard, len(shards))
	for i := range shards {
		shardInfo := shards[i].GetInfo()
		shard := &proto.Shard{
			Id:       shardInfo.ShardId,
			Epoch:    shardInfo.Epoch,
			InoLimit: shardInfo.InoLimit,
			InoUsed:  shardInfo.InoUsed,
			LeaderId: shardInfo.Leader,
		}
		for _, nodeId := range shardInfo.Replicates {
			node, err := c.cluster.GetNode(ctx, nodeId)
			if err != nil {
				return nil, err
			}
			shard.Nodes = append(shard.Nodes, node)
		}
		meta.Shards[i] = shard
	}

	return nil, nil
}

func (c *catalog) GetSpaceByName(ctx context.Context, name string) (*proto.SpaceMeta, error) {
	space := c.spaces.GetByName(name)
	return c.GetSpace(ctx, space.id)
}

func (c *catalog) Report(ctx context.Context, nodeId uint32, infos []*proto.ShardReport) ([]*proto.ShardTask, error) {
	for _, reportInfo := range infos {
		if c.spaces.Get(reportInfo.Sid) == nil {
			return nil, apierrors.ErrSpaceNotExist
		}
	}

	args := shardReportsArgs{
		nodeId: nodeId,
		infos:  infos,
	}
	data, err := json.Marshal(args)
	if err != nil {
		return nil, errors.Info(err, "json marshal data failed")
	}

	if _, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpShardReport,
		Data:       data,
		WithResult: true,
	}); err != nil {
		return nil, err
	}

	// TODO: remove this function call after invoke raft
	ret, err := c.applyShardReport(ctx, data)
	if err != nil {
		return nil, err
	}

	if len(ret.maybeExpandingSpaces) > 0 {
		for _, space := range ret.maybeExpandingSpaces {
			c.taskMgr.Send(ctx, &task{
				sid: space.id,
				typ: taskTypeExpandSpace,
			})
		}
	}

	return ret.tasks, nil
}

func (c *catalog) GetCatalogChanges(ctx context.Context, routerVersion uint64, nodeId uint32) (ret []*proto.CatalogChangeItem, err error) {
	items := c.routeMgr.GetRouteItems(ctx, routerVersion)
	if items == nil {
		// TODO: get all catalog
		return nil, nil
	}

	ret = make([]*proto.CatalogChangeItem, len(items))
	for i := range items {
		ret[i] = &proto.CatalogChangeItem{
			RouteVersion: items[i].RouteVersion,
			Type:         items[i].Type,
		}

		switch items[i].Type {
		case proto.CatalogChangeType_AddSpace:
			itemDetail := items[i].ItemDetail.(*routeItemSpaceAdd)
			space := c.spaces.Get(itemDetail.Sid)
			spaceInfo := space.GetInfo()
			err = ret[i].Item.MarshalFrom(&proto.CatalogChangeSpaceAdd{
				Sid:         itemDetail.Sid,
				Name:        spaceInfo.Name,
				Type:        spaceInfo.Type,
				FixedFields: internalFieldMetasToProtoFieldMetas(spaceInfo.FixedFields),
			})
		case proto.CatalogChangeType_DeleteSpace:
			itemDetail := items[i].ItemDetail.(*routeItemSpaceDelete)
			err = ret[i].Item.MarshalFrom(&proto.CatalogChangeSpaceDelete{
				Sid: itemDetail.Sid,
			})
		case proto.CatalogChangeType_AddShard:
			itemDetail := items[i].ItemDetail.(*routeItemShardAdd)
			space := c.spaces.Get(itemDetail.Sid)
			shard := space.GetShard(itemDetail.ShardId)
			shardInfo := shard.GetInfo()
			err = ret[i].Item.MarshalFrom(&proto.CatalogChangeShardAdd{
				Sid:        itemDetail.Sid,
				ShardId:    itemDetail.ShardId,
				Epoch:      shardInfo.Epoch,
				InoLimit:   shardInfo.InoLimit,
				Replicates: shardInfo.Replicates,
			})
		}

		if err != nil {
			return
		}
	}

	return
}

func (c *catalog) Close() {
	close(c.done)
}

func (c *catalog) createSpaceTask(ctx context.Context, sid uint64, _ []byte) error {
	span := trace.SpanFromContext(ctx)
	space := c.spaces.Get(sid)

	if space.IsNormal() {
		span.Warnf("space[%d] creation already done", sid)
		return nil
	}

	if space.IsInit(true) {
		if err := c.createSpaceShards(ctx, space, 1, space.info.DesiredShardNum, RaftOpInitSpaceShards); err != nil {
			span.Warnf("initial space's shards failed: %s", errors.Detail(err))
			return err
		}
	}

	if space.IsUpdateRoute(true) {
		if err := c.updateSpaceRoute(ctx, space); err != nil {
			span.Warnf("update space's route failed: %s", errors.Detail(err))
			return err
		}
	}

	return nil
}

func (c *catalog) expandSpaceTask(ctx context.Context, sid uint64, _ []byte) error {
	space := c.spaces.Get(sid)

	if space.IsExpandUpdateRoute(true) {
		return c.expandSpaceUpdateRoute(ctx, space)
	}
	return c.createSpaceShards(ctx, space, space.GetCurrentShardId(), c.cfg.ExpandShardsNumPerSpace, RaftOpExpandSpaceCreateShards)
}

func (c *catalog) createSpaceShards(ctx context.Context, space *space, startShardId uint32, desiredShardsNum uint32, op raft.Op) error {
	shardInfos := make([]*shardInfo, 0, desiredShardsNum)

	for shardId := startShardId; shardId <= desiredShardsNum; shardId++ {
		// alloc nodes
		nodeInfos, err := c.cluster.Alloc(ctx, &cluster.AllocArgs{
			Count: c.cfg.ShardReplicateNum,
			Role:  proto.NodeRole_ShardServer,
		})
		if err != nil {
			return errors.Info(err, "alloc shard server nodes failed")
		}
		nodes := make([]uint32, len(nodeInfos))
		for i := range nodeInfos {
			nodes[i] = nodeInfos[i].Id
		}

		// TODO: shuffle the nodes for raft group leader election

		// create shards
		for i := range nodes {
			client, err := c.cluster.GetClient(ctx, nodes[i])
			if err != nil {
				return errors.Info(err, "get client failed")
			}

			if _, err := client.AddShard(ctx, &proto.AddShardRequest{
				Sid:        space.id,
				SpaceName:  space.info.Name,
				ShardId:    shardId,
				InoLimit:   c.cfg.InoLimitPerShard,
				Replicates: nodes,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("add shard to node[%d] failed", nodes[i]))
			}
		}

		shardInfos = append(shardInfos, &shardInfo{
			ShardId:    shardId,
			InoLimit:   c.cfg.InoLimitPerShard,
			Replicates: nodes,
		})
	}

	// save shard and modify space's status
	args := &createSpaceShardsArgs{
		Sid:        space.id,
		ShardInfos: shardInfos,
	}
	data, err := json.Marshal(args)
	if err != nil {
		return errors.Info(err, "json marshal data failed")
	}

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module: module,
		Op:     op,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose initial space shards failed")
	}

	// TODO: remove this function call after invoke raft
	switch op {
	case RaftOpInitSpaceShards:
		if err := c.applyInitSpaceShards(ctx, data); err != nil {
			return err
		}
	case RaftOpExpandSpaceCreateShards:
		if err := c.applyExpandSpaceShards(ctx, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *catalog) updateSpaceRoute(ctx context.Context, space *space) error {
	shards := space.GetAllShards()

	baseEpoch, _, err := c.genNewEpoch(ctx, len(shards)+1)
	if err != nil {
		return errors.Info(err, "generate new route version")
	}

	shardInfos := make([]*shardInfo, 0, len(shards))
	for i, shard := range shards {
		shardInfo := shard.GetInfo()
		shardInfo.Epoch = baseEpoch + 1 + uint64(i)

		// update shard's epoch
		for _, nodeId := range shardInfo.Replicates {
			client, err := c.cluster.GetClient(ctx, nodeId)
			if err != nil {
				return errors.Info(err, "get client failed")
			}

			if _, err := client.UpdateShard(ctx, &proto.UpdateShardRequest{
				Sid:     space.id,
				ShardId: shardInfo.ShardId,
				Epoch:   shardInfo.Epoch,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("update shard to node[%d] failed", nodeId))
			}
		}

		shardInfos = append(shardInfos, shardInfo)
	}

	// save shard and modify space's status
	args := &updateSpaceRouteArgs{
		SpaceEpoch: baseEpoch,
		createSpaceShardsArgs: createSpaceShardsArgs{
			Sid:        space.id,
			ShardInfos: shardInfos,
		},
	}
	data, err := json.Marshal(args)
	if err != nil {
		return errors.Info(err, "json marshal data failed")
	}

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module: module,
		Op:     RaftOpUpdateSpaceRoute,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose update space route failed")
	}

	// TODO: remove this function call after invoke raft
	if err := c.applyInitSpaceShards(ctx, data); err != nil {
		return err
	}

	return nil
}

func (c *catalog) expandSpaceUpdateRoute(ctx context.Context, space *space) error {
	shards := space.GetExpandingShards()

	baseEpoch, _, err := c.genNewEpoch(ctx, len(shards))
	if err != nil {
		return errors.Info(err, "generate new route version")
	}

	shardInfos := make([]*shardInfo, 0, len(shards))
	for i, shard := range shards {
		shardInfo := shard.GetInfo()
		shardInfo.Epoch = baseEpoch + uint64(i)

		// update shard's epoch
		for _, nodeId := range shardInfo.Replicates {
			client, err := c.cluster.GetClient(ctx, nodeId)
			if err != nil {
				return errors.Info(err, "get client failed")
			}

			if _, err := client.UpdateShard(ctx, &proto.UpdateShardRequest{
				Sid:     space.id,
				ShardId: shardInfo.ShardId,
				Epoch:   shardInfo.Epoch,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("update shard to node[%d] failed", nodeId))
			}
		}

		shardInfos = append(shardInfos, shardInfo)
	}

	// save shard and modify space's status
	args := &createSpaceShardsArgs{
		Sid:        space.id,
		ShardInfos: shardInfos,
	}
	data, err := json.Marshal(args)
	if err != nil {
		return errors.Info(err, "json marshal data failed")
	}

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module: module,
		Op:     RaftOpExpandSpaceUpdateRoute,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose update space route failed")
	}

	// TODO: remove this function call after invoke raft
	if err := c.applyExpandSpaceUpdateRoute(ctx, data); err != nil {
		return err
	}

	return nil
}

func (c *catalog) genNewEpoch(ctx context.Context, step int) (base, new uint64, err error) {
	base, new, err = c.idGenerator.Alloc(ctx, epochIdName, step)
	return
}

// concurrentSpaces is an effective data struct (concurrent map implements)
type concurrentSpaces struct {
	num     uint32
	idMap   map[uint32]map[uint64]*space
	nameMap map[uint32]map[string]*space
	locks   map[uint32]*sync.RWMutex
}

func newConcurrentSpaces(splitMapNum uint32) *concurrentSpaces {
	spaces := &concurrentSpaces{
		num:     splitMapNum,
		idMap:   make(map[uint32]map[uint64]*space),
		nameMap: make(map[uint32]map[string]*space),
		locks:   make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		spaces.locks[i] = &sync.RWMutex{}
		spaces.idMap[i] = make(map[uint64]*space)
		spaces.nameMap[i] = make(map[string]*space)
	}
	return spaces
}

// Get space from concurrentSpaces
func (s *concurrentSpaces) Get(sid uint64) *space {
	idx := uint32(sid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.idMap[idx][sid]
}

func (s *concurrentSpaces) GetNoLock(sid uint64) *space {
	idx := uint32(sid) % s.num
	return s.idMap[idx][sid]
}

func (s *concurrentSpaces) GetByName(name string) *space {
	idx := s.nameCharSum(name) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.nameMap[idx][name]
}

func (s *concurrentSpaces) GetByNameNoLock(name string) *space {
	idx := s.nameCharSum(name) % s.num
	return s.nameMap[idx][name]
}

// Put new space into shardedSpace
func (s *concurrentSpaces) Put(v *space) {
	id := v.id
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	s.idMap[idx][id] = v
	s.locks[idx].Unlock()

	idx = s.nameCharSum(v.info.Name) % s.num
	s.locks[idx].Lock()
	s.nameMap[idx][v.info.Name] = v
	s.locks[idx].Unlock()
}

// Delete space into shardedSpace
func (s *concurrentSpaces) Delete(id uint64) {
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	v := s.idMap[idx][id]
	delete(s.idMap[idx], id)
	s.locks[idx].Unlock()

	idx = s.nameCharSum(v.info.Name) % s.num
	s.locks[idx].Lock()
	s.nameMap[idx][v.info.Name] = v
	s.locks[idx].Unlock()
}

// Range concurrentSpaces, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentSpaces) Range(f func(v *space) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.idMap[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}

func (s *concurrentSpaces) GetLock(sid uint64) *sync.RWMutex {
	idx := uint32(sid) % s.num
	return s.locks[idx]
}

func (s *concurrentSpaces) GetLockByName(name string) *sync.RWMutex {
	idx := s.nameCharSum(name) % s.num
	return s.locks[idx]
}

func (s *concurrentSpaces) nameCharSum(name string) (ret uint32) {
	for i := range name {
		ret += uint32(name[i])
	}
	return
}
