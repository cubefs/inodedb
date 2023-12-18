package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cubefs/inodedb/master/store"
	"github.com/gogo/protobuf/types"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
)

const (
	spaceIdName = "space"
	epochIdName = "epoch"
)

const (
	MaxDesiredShardsNum = 1024

	defaultSplitMapNum             = 16
	defaultTaskPoolNum             = 16
	defaultShardReplicateNum       = 3
	defaultInoLimitPerShard        = 4 << 20
	defaultInoUsedThreshold        = 0.6
	defaultExpandShardsNumPerSpace = 32
)

type Catalog interface {
	CreateSpace(ctx context.Context, SpaceName string, SpaceType proto.SpaceType, DesiredShards uint32, FixedFields []proto.FieldMeta) error
	DeleteSpace(ctx context.Context, sid uint64) error
	DeleteSpaceByName(ctx context.Context, name string) error
	GetSpace(ctx context.Context, sid uint64) (*proto.SpaceMeta, error)
	GetSpaceByName(ctx context.Context, name string) (*proto.SpaceMeta, error)
	Report(ctx context.Context, nodeId uint32, infos []proto.ShardReport) ([]proto.ShardTask, error)
	GetCatalogChanges(ctx context.Context, routerVersion uint64, nodeId uint32) (uint64, []proto.CatalogChangeItem, error)
	GetSM() raft.Applier
	Close()
}

type Config struct {
	ShardReplicateNum       int                     `json:"shard_replicate_num"`
	InoLimitPerShard        uint64                  `json:"ino_limit_per_shard"`
	ExpandShardsNumPerSpace uint32                  `json:"expand_shards_num_per_space"`
	AZs                     []string                `json:"azs"`
	IdGenerator             idgenerator.IDGenerator `json:"-"`
	Store                   *store.Store            `json:"-"`
	Cluster                 cluster.Cluster         `json:"-"`
	RaftGroup               raft.Group              `json:"-"`
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
		cfg         *Config

		done chan struct{}
		lock sync.RWMutex
	}

	createSpaceShardsArgs struct {
		Sid        uint64
		ShardInfos []*shardInfo
	}
	updateSpaceRouteArgs struct {
		Sid        uint64
		ShardInfos []*shardInfo
		SpaceEpoch uint64
	}
	deleteSpaceArgs struct {
		Sid uint64
	}
	shardReportsArgs struct {
		nodeId uint32
		infos  []proto.ShardReport
	}
	shardReportsResult struct {
		tasks                []proto.ShardTask
		maybeExpandingSpaces []*space
	}
)

func NewCatalog(ctx context.Context, cfg *Config) Catalog {
	span := trace.SpanFromContext(ctx)
	err := initConfig(cfg)
	if err != nil {
		span.Fatalf("init config failed for catalog, err: %s", err)
	}

	c := &catalog{
		spaces:      newConcurrentSpaces(defaultSplitMapNum),
		idGenerator: cfg.IdGenerator,
		storage:     newStorage(cfg.Store),
		cluster:     cfg.Cluster,
		raftGroup:   cfg.RaftGroup,
		taskMgr:     newTaskMgr(defaultTaskPoolNum),
		done:        make(chan struct{}),
		cfg:         cfg,
	}

	c.taskMgr.Register(taskTypeCreateSpace, c.createSpaceTask)
	c.taskMgr.Register(taskTypeExpandSpace, c.maybeExpandSpaceTask)
	if err := c.Load(ctx); err != nil {
		span.Fatalf("load catalog data failed: %s", err)
	}

	c.taskMgr.Start()

	return c
}

func (c *catalog) GetSM() raft.Applier {
	return c
}

func (c *catalog) Load(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)
	c.spaces = newConcurrentSpaces(defaultSplitMapNum)
	spaceInfos, err := c.storage.ListSpaces(ctx)
	if err != nil {
		return errors.Info(err, "list spaces failed")
	}

	for _, spaceInfo := range spaceInfos {
		space := newSpace(spaceInfo)
		c.spaces.Put(space)

		shardInfos, err := c.storage.ListShards(ctx, space.id)
		span.Infof("shards length: %d", len(shardInfos))
		if err != nil {
			return errors.Info(err, "list space shards failed")
		}

		for _, shardInfo := range shardInfos {
			space.PutShard(newShard(shardInfo))
		}

		span.Infof("load space: %+v", space.GetInfo())
		if space.IsInit(true) || space.IsUpdateRoute(true) {
			c.taskMgr.Send(ctx, &task{
				sid: space.id,
				typ: taskTypeCreateSpace,
			})
		}
	}

	c.routeMgr, err = newRouteMgr(ctx, defaultRouteItemTruncateIntervalNum, c.storage)
	if err != nil {
		return errors.Info(err, "new route manager failed")
	}

	return nil
}

// CreateSpace(ctx context.Context, SpaceName string, SpaceType proto.SpaceType, DesiredShards uint32, FixedFields []*proto.FieldMeta) error
func (c *catalog) CreateSpace(
	ctx context.Context,
	spaceName string,
	spaceType proto.SpaceType,
	desiredShards uint32,
	fixedFields []proto.FieldMeta,
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

	if _, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpCreateSpace,
		Data:   data,
	}); err != nil {
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
	if _, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpDeleteSpace,
		Data:   data,
	}); err != nil {
		return err
	}

	return nil
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
	meta.Shards = make([]proto.Shard, len(shards))
	for i := range shards {
		shardInfo := shards[i].GetInfo()
		shard := proto.Shard{
			ShardID:  shardInfo.ShardId,
			Epoch:    shardInfo.Epoch,
			InoLimit: shardInfo.InoLimit,
			InoUsed:  shardInfo.InoUsed,
			LeaderID: shardInfo.Leader,
		}
		for _, node := range shardInfo.Nodes {
			shard.Nodes = append(shard.Nodes, proto.ShardNode{DiskID: node.ID, Learner: node.Learner})
		}
		meta.Shards[i] = shard
	}

	return meta, nil
}

func (c *catalog) GetSpaceByName(ctx context.Context, name string) (*proto.SpaceMeta, error) {
	space := c.spaces.GetByName(name)
	return c.GetSpace(ctx, space.id)
}

func (c *catalog) Report(ctx context.Context, nodeId uint32, infos []proto.ShardReport) ([]proto.ShardTask, error) {
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

	resp := raft.ProposalResponse{}
	if resp, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpShardReport,
		Data:   data,
	}); err != nil {
		return nil, err
	}

	ret := resp.Data.(*shardReportsResult)
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

func (c *catalog) GetCatalogChanges(ctx context.Context, fromRouterVersion uint64, nodeId uint32) (routeVersion uint64, ret []proto.CatalogChangeItem, err error) {
	span := trace.SpanFromContext(ctx)
	span.Infof("get catalog changes from %d", fromRouterVersion)
	var (
		items    []*routeItemInfo
		isLatest bool
	)
	if fromRouterVersion > 0 {
		items, isLatest = c.routeMgr.GetRouteItems(ctx, fromRouterVersion)
	}

	if items == nil && !isLatest {
		// get all catalog
		routeVersion = c.routeMgr.GetRouteVersion()
		spaces := c.spaces.List()
		for _, space := range spaces {
			if !space.IsNormal() {
				continue
			}

			items = append(items, &routeItemInfo{
				Type:       proto.CatalogChangeItem_AddSpace,
				ItemDetail: &routeItemSpaceAdd{Sid: space.id},
			})
			span.Infof("space: %+v", space.GetInfo())
			shards := space.shards.List()
			span.Infof("space shards: %d", len(shards))
			for _, shard := range shards {
				items = append(items, &routeItemInfo{
					Type: proto.CatalogChangeItem_AddShard,
					ItemDetail: &routeItemShardAdd{
						Sid:     space.id,
						ShardId: shard.id,
					},
				})
			}
		}
	}

	ret = make([]proto.CatalogChangeItem, len(items))
	for i := range items {
		ret[i] = proto.CatalogChangeItem{
			RouteVersion: items[i].RouteVersion,
			Type:         items[i].Type,
		}

		switch items[i].Type {
		case proto.CatalogChangeItem_AddSpace:
			itemDetail := items[i].ItemDetail.(*routeItemSpaceAdd)
			space := c.spaces.Get(itemDetail.Sid)
			spaceInfo := space.GetInfo()
			spaceItem := &proto.CatalogChangeSpaceAdd{
				Sid:         itemDetail.Sid,
				Name:        spaceInfo.Name,
				Type:        spaceInfo.Type,
				FixedFields: internalFieldMetasToProtoFieldMetas(spaceInfo.FixedFields),
			}
			ret[i].Item, err = types.MarshalAny(spaceItem)
		case proto.CatalogChangeItem_DeleteSpace:
			itemDetail := items[i].ItemDetail.(*routeItemSpaceDelete)
			space := c.spaces.Get(itemDetail.Sid)
			spaceInfo := space.GetInfo()

			spaceItem := &proto.CatalogChangeSpaceDelete{
				Sid:  itemDetail.Sid,
				Name: spaceInfo.Name,
			}
			ret[i].Item, err = types.MarshalAny(spaceItem)
		case proto.CatalogChangeItem_AddShard:
			itemDetail := items[i].ItemDetail.(*routeItemShardAdd)
			space := c.spaces.Get(itemDetail.Sid)
			shard := space.GetShard(itemDetail.ShardId)
			shardInfo := shard.GetInfo()

			spaceItem := &proto.CatalogChangeShardAdd{
				Sid:      itemDetail.Sid,
				Name:     space.GetInfo().Name,
				ShardID:  itemDetail.ShardId,
				Epoch:    shardInfo.Epoch,
				InoLimit: shardInfo.InoLimit,
				Leader:   shardInfo.Leader,
				Nodes:    internalShardNodesToProtoShardNodes(shardInfo.Nodes),
			}
			ret[i].Item, err = types.MarshalAny(spaceItem)
		default:

		}

		if err != nil {
			return
		}
	}

	if routeVersion == 0 && len(ret) > 0 {
		routeVersion = ret[len(ret)-1].RouteVersion
	}

	return
}

func (c *catalog) Close() {
	close(c.done)
}

func (c *catalog) createSpaceTask(ctx context.Context, sid uint64, _ []byte) error {
	span := trace.SpanFromContext(ctx)
	span.Infof("start create space[%d] task", sid)
	space := c.spaces.Get(sid)

	if space.IsNormal() {
		span.Warnf("space[%d] creation already done", sid)
		return nil
	}

	if space.IsInit(true) {
		span.Infof("start create space shards")
		if err := c.createSpaceShards(ctx, space, 1, space.GetInfo().DesiredShardNum, RaftOpInitSpaceShards); err != nil {
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

func (c *catalog) maybeExpandSpaceTask(ctx context.Context, sid uint64, _ []byte) error {
	span := trace.SpanFromContext(ctx)
	space := c.spaces.Get(sid)

	// check if space need to expand
	allShards := space.GetAllShards()
	latestShards := allShards[len(allShards)-int(c.cfg.ExpandShardsNumPerSpace):]
	totalIno := uint64(0)
	totalInoUsed := uint64(0)
	for _, shard := range latestShards {
		info := shard.GetInfo()
		totalIno += info.InoLimit
		totalInoUsed += info.InoUsed
	}
	if float64(totalInoUsed)/float64(totalIno) < defaultInoUsedThreshold {
		span.Infof("space[%d] does not need to expand", space.id)
		return nil
	}

	span.Infof("start expand space[%d]", space.id)
	if space.IsExpandUpdateRoute(true) {
		return c.expandSpaceUpdateRoute(ctx, space)
	}
	return c.createSpaceShards(ctx, space, space.GetCurrentShardId(), c.cfg.ExpandShardsNumPerSpace, RaftOpExpandSpaceCreateShards)
}

func (c *catalog) createSpaceShards(ctx context.Context, space *space, startShardId uint32, desiredShardsNum uint32, op uint32) error {
	span := trace.SpanFromContext(ctx)
	shardInfos := make([]*shardInfo, 0, desiredShardsNum)

	for shardId := startShardId; shardId <= desiredShardsNum; shardId++ {
		span.Info("start alloc nodes")
		// alloc nodes
		nodeInfos, err := c.cluster.Alloc(ctx, &cluster.AllocArgs{
			AZ:    c.cfg.AZs[0],
			Count: c.cfg.ShardReplicateNum,
			Role:  proto.NodeRole_ShardServer,
		})
		span.Infof("alloc result: %+v", nodeInfos)
		if err != nil {
			return errors.Info(err, "alloc shard server nodes failed")
		}

		protoShardNodes := make([]proto.ShardNode, len(nodeInfos))
		internalShardNodes := make([]shardNode, len(nodeInfos))
		for i := range nodeInfos {
			protoShardNodes[i] = proto.ShardNode{
				DiskID:  nodeInfos[i].DiskID,
				Learner: false,
			}
			internalShardNodes[i] = shardNode{
				ID:      nodeInfos[i].DiskID,
				Learner: false,
			}
		}

		// TODO: shuffle the nodes for raft group leader election

		// create shards
		for i := range protoShardNodes {
			span.Info("start get client")
			client, err := c.cluster.GetClient(ctx, protoShardNodes[i].DiskID)
			if err != nil {
				return errors.Info(err, "get client failed")
			}
			span.Info("get client success")

			if _, err := client.AddShard(ctx, &proto.AddShardRequest{
				Sid: space.id,
				ShardID:  shardId,
				InoLimit: c.cfg.InoLimitPerShard,
				Nodes:    protoShardNodes,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("add shard to node[%d] failed", protoShardNodes[i]))
			}
			span.Info("add shard success")
		}

		shardInfos = append(shardInfos, &shardInfo{
			ShardId:  shardId,
			InoLimit: c.cfg.InoLimitPerShard,
			Nodes:    internalShardNodes,
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

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     op,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose initial space shards failed")
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
		for _, node := range shardInfo.Nodes {
			client, err := c.cluster.GetClient(ctx, node.ID)
			if err != nil {
				return errors.Info(err, "get client failed")
			}

			if _, err := client.UpdateShard(ctx, &proto.UpdateShardRequest{
				// SpaceName: space.GetInfo().Name,
				Sid:     space.GetInfo().Sid,
				ShardID: shardInfo.ShardId,
				// Epoch:     shardInfo.Epoch,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("update shard to node[%d] failed", node.ID))
			}
		}

		shardInfos = append(shardInfos, shardInfo)
	}

	// save shard and modify space's status
	args := &updateSpaceRouteArgs{
		SpaceEpoch: baseEpoch,
		Sid:        space.id,
		ShardInfos: shardInfos,
	}
	data, err := json.Marshal(args)
	if err != nil {
		return errors.Info(err, "json marshal data failed")
	}

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpUpdateSpaceRoute,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose update space route failed")
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
		for _, node := range shardInfo.Nodes {
			client, err := c.cluster.GetClient(ctx, node.ID)
			if err != nil {
				return errors.Info(err, "get client failed")
			}

			if _, err := client.UpdateShard(ctx, &proto.UpdateShardRequest{
				// SpaceName: space.GetInfo().Name,
				Sid:     space.GetInfo().Sid,
				ShardID: shardInfo.ShardId,
				// Epoch:     shardInfo.Epoch,
			}); err != nil {
				return errors.Info(err, fmt.Sprintf("update shard to node[%d] failed", node.ID))
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

	if _, err := c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpExpandSpaceUpdateRoute,
		Data:   data,
	}); err != nil {
		return errors.Info(err, "propose update space route failed")
	}

	return nil
}

func (c *catalog) genNewEpoch(ctx context.Context, step int) (base, new uint64, err error) {
	base, new, err = c.idGenerator.Alloc(ctx, epochIdName, step)
	return
}

func initConfig(cfg *Config) error {
	if len(cfg.AZs) == 0 {
		return errors.New("at least config one az for master")
	}
	if cfg.ShardReplicateNum <= 0 {
		cfg.ShardReplicateNum = defaultShardReplicateNum
	}
	if cfg.ExpandShardsNumPerSpace <= 0 {
		cfg.ExpandShardsNumPerSpace = defaultExpandShardsNumPerSpace
	}
	if cfg.InoLimitPerShard <= 0 {
		cfg.InoLimitPerShard = defaultInoLimitPerShard
	}
	return nil
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

// Put new space into shardedSpace
func (s *concurrentSpaces) PutNoLock(v *space) {
	id := v.id
	idx := uint32(id) % s.num
	s.idMap[idx][id] = v

	idx = s.nameCharSum(v.info.Name) % s.num
	s.nameMap[idx][v.info.Name] = v
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

func (s *concurrentSpaces) Len() uint64 {
	ret := uint64(0)
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		ret += uint64(len(s.idMap[i]))
		l.RUnlock()
	}
	return ret
}

func (s *concurrentSpaces) List() []*space {
	ret := make([]*space, 0, s.Len()*2)
	s.Range(func(v *space) error {
		ret = append(ret, v)
		return nil
	})
	return ret
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
