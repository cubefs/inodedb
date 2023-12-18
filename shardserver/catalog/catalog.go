package catalog

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
	"github.com/cubefs/inodedb/client"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/store"
	"github.com/gogo/protobuf/types"
)

const defaultTaskPoolSize = 64

type (
	Config struct {
		StoreConfig     store.Config        `json:"store_config"`
		MasterConfig    client.MasterConfig `json:"master_config"`
		NodeConfig      proto.Node          `json:"node_config"`
		RaftConfig      raft.Config         `json:"raft_config"`
		ShardBaseConfig ShardBaseConfig     `json:"shard_base_config"`
		Disks           []string            `json:"disks_config"`
	}
)

type Catalog struct {
	routeVersion uint64
	spaces       sync.Map
	done         chan struct{}

	disks     sync.Map
	transport *transport
	taskPool  taskpool.TaskPool
	cfg       *Config
}

func NewCatalog(ctx context.Context, cfg *Config) *Catalog {
	initConfig(cfg)
	span := trace.SpanFromContext(ctx)

	masterClient, err := client.NewMasterClient(&cfg.MasterConfig)
	if err != nil {
		span.Fatalf("new master client failed: %s", err)
	}
	transport := newTransport(masterClient, &cfg.NodeConfig)
	// register node
	if err := transport.Register(ctx); err != nil {
		span.Fatalf("register shard server failed: %s", err)
	}
	nodeID := transport.GetMyself().ID

	catalog := &Catalog{
		transport: transport,
		done:      make(chan struct{}),
		taskPool:  taskpool.New(defaultTaskPoolSize, defaultTaskPoolSize),
		cfg:       cfg,
	}

	catalog.initRaftConfig()
	catalog.initDisk(ctx, nodeID)
	if err := catalog.initRoute(ctx); err != nil {
		span.Fatalf("update route failed: %s", errors.Detail(err))
	}

	catalog.transport.StartHeartbeat(ctx)
	go catalog.loop(ctx)
	return catalog
}

func (c *Catalog) AddShard(ctx context.Context, diskID proto.DiskID, sid proto.Sid, shardID proto.ShardID, epoch uint64, inoLimit uint64, nodes []proto.ShardNode) error {
	span := trace.SpanFromContext(ctx)
	_, ok := c.spaces.Load(sid)
	if !ok {
		if err := c.updateSpace(ctx, sid); err != nil {
			span.Warnf("update route failed: %s", err)
			return apierrors.ErrSpaceDoesNotExist
		}
	}

	disk, err := c.getDisk(diskID)
	if err != nil {
		return err
	}

	return disk.AddShard(ctx, sid, shardID, epoch, inoLimit, nodes)
}

// UpdateShard update shard info
// todo: update shard nodes support
func (c *Catalog) UpdateShard(ctx context.Context, diskID proto.DiskID, sid proto.Sid, shardID proto.ShardID, epoch uint64) error {
	disk, err := c.getDisk(diskID)
	if err != nil {
		return err
	}

	return disk.UpdateShard(ctx, sid, shardID, epoch)
}

func (c *Catalog) GetShardInfo(ctx context.Context, diskID proto.DiskID, sid proto.Sid, shardID proto.ShardID) (ret proto.Shard, err error) {
	disk, err := c.getDisk(diskID)
	if err != nil {
		return
	}
	shard, err := disk.GetShard(sid, shardID)
	if err != nil {
		return
	}

	shardStat := shard.Stats()
	// transform into external nodes
	nodes := make([]proto.ShardNode, 0, len(shardStat.nodes))
	for _, node := range shardStat.nodes {
		nodes = append(nodes, proto.ShardNode{
			DiskID:  node.DiskID,
			Learner: node.Learner,
		})
	}

	return proto.Shard{
		ShardID:  shard.shardID,
		InoLimit: shardStat.inoLimit,
		InoUsed:  shardStat.inoUsed,
		Nodes:    nodes,
	}, nil
}

func (c *Catalog) GetSpace(ctx context.Context, sid proto.Sid) (*Space, error) {
	return c.getSpace(sid)
}

func (c *Catalog) GetNodeInfo() *proto.Node {
	return c.transport.GetMyself()
}

func (c *Catalog) GetShardBaseConfig() *ShardBaseConfig {
	return &c.cfg.ShardBaseConfig
}

func (c *Catalog) loop(ctx context.Context) {
	reportTicker := time.NewTicker(60 * time.Second)
	routeUpdateTicker := time.NewTicker(5 * time.Second)
	checkpointTicker := time.NewTicker(1 * time.Minute)

	defer func() {
		reportTicker.Stop()
		routeUpdateTicker.Stop()
		checkpointTicker.Stop()
	}()

	for {
		select {
		case <-reportTicker.C:
			span, ctx := trace.StartSpanFromContext(ctx, "")
			shardReports := c.getAlteredShardReports()
			tasks, err := c.transport.Report(ctx, shardReports)
			if err != nil {
				span.Warnf("shard report failed: %s", err)
				continue
			}
			for _, task := range tasks {
				if err := (*catalogTask)(c).executeShardTask(ctx, task); err != nil {
					span.Errorf("execute shard task[%+v] failed: %s", task, errors.Detail(err))
					continue
				}
			}

			reportTicker.Reset(time.Duration(60+rand.Intn(10)) * time.Second)
		case <-routeUpdateTicker.C:
			span, ctx := trace.StartSpanFromContext(ctx, "")
			if err := c.initRoute(ctx); err != nil {
				span.Warnf("update route failed: %s", err)
			}
			routeUpdateTicker.Reset(time.Duration(5+rand.Intn(5)) * time.Second)
		case <-checkpointTicker.C:
			span, ctx := trace.StartSpanFromContext(ctx, "")
			shardTasks := c.getAlteredShardCheckpointTasks()
			for _, task := range shardTasks {
				if err := (*catalogTask)(c).executeShardTask(ctx, task); err != nil {
					span.Errorf("execute shard task[%+v] failed: %s", task, errors.Detail(err))
					continue
				}
			}
		case <-c.done:
			return
		}
	}
}

func (c *Catalog) getDisk(diskID proto.DiskID) (*disk, error) {
	v, ok := c.disks.Load(diskID)
	if !ok {
		return nil, apierrors.ErrDiskNotExist
	}

	disk := v.(*disk)
	return disk, nil
}

func (c *Catalog) getSpace(sid proto.Sid) (*Space, error) {
	v, ok := c.spaces.Load(sid)
	if !ok {
		return nil, apierrors.ErrSpaceDoesNotExist
	}

	space := v.(*Space)
	return space, nil
}

func (c *Catalog) getShard(diskID proto.DiskID, sid proto.Sid, shardID proto.ShardID) (*shard, error) {
	disk, err := c.getDisk(diskID)
	if err != nil {
		return nil, err
	}
	shard, err := disk.GetShard(sid, shardID)
	if err != nil {
		return nil, err
	}

	return shard, nil
}

// TODO: get altered shards, optimized the load of master
func (c *Catalog) getAlteredShardReports() []proto.ShardReport {
	ret := make([]proto.ShardReport, 0, 1<<10)

	c.disks.Range(func(key, value interface{}) bool {
		disk := value.(*disk)
		disk.RangeShard(func(s *shard) bool {
			stats := s.Stats()
			ret = append(ret, proto.ShardReport{
				Sid: s.sid,
				Shard: proto.Shard{
					Epoch:    stats.epoch,
					ShardID:  s.shardID,
					LeaderID: stats.leader,
					InoLimit: stats.inoLimit,
					InoUsed:  stats.inoUsed,
				},
			})
			return true
		})
		return true
	})

	return ret
}

// TODO: get altered shards, optimized the load of master
func (c *Catalog) getAlteredShardCheckpointTasks() []proto.ShardTask {
	ret := make([]proto.ShardTask, 0, 1<<10)
	c.disks.Range(func(key, value interface{}) bool {
		disk := value.(*disk)
		disk.RangeShard(func(s *shard) bool {
			ret = append(ret, proto.ShardTask{
				Type:    proto.ShardTask_Checkpoint,
				Sid:     s.sid,
				ShardID: s.shardID,
			})
			return true
		})
		return true
	})
	return ret
}

func (c *Catalog) updateSpace(ctx context.Context, sid proto.Sid) error {
	spaceMeta, err := c.transport.GetSpace(ctx, sid)
	if err != nil {
		return err
	}

	fixedFields := make(map[string]proto.FieldMeta, len(spaceMeta.FixedFields))
	for _, field := range spaceMeta.FixedFields {
		fixedFields[field.Name] = field
	}

	space := &Space{
		sid:         spaceMeta.Sid,
		name:        spaceMeta.Name,
		spaceType:   spaceMeta.Type,
		fixedFields: fixedFields,
		locateShard: c.getShard,
	}
	if _, loaded := c.spaces.LoadOrStore(spaceMeta.Sid, space); loaded {
		return nil
	}

	// c.spaceIdToNames.Store(spaceMeta.Sid, spaceMeta.Name)
	return nil
}

func (c *Catalog) initRoute(ctx context.Context) error {
	routeVersion, changes, err := c.transport.GetRouteUpdate(ctx, c.getRouteVersion())
	if err != nil {
		return errors.Info(err, "get route update failed")
	}

	for _, routeItem := range changes {
		switch routeItem.Type {
		case proto.CatalogChangeItem_AddSpace:
			spaceItem := new(proto.CatalogChangeSpaceAdd)
			if err := types.UnmarshalAny(routeItem.Item, spaceItem); err != nil {
				return errors.Info(err, "unmarshal add Space item failed")
			}

			fixedFields := make(map[string]proto.FieldMeta, len(spaceItem.FixedFields))
			for _, field := range spaceItem.FixedFields {
				fixedFields[field.Name] = field
			}

			space := &Space{
				sid:         spaceItem.Sid,
				name:        spaceItem.Name,
				spaceType:   spaceItem.Type,
				fixedFields: fixedFields,
				locateShard: c.getShard,
			}
			if _, loaded := c.spaces.LoadOrStore(spaceItem.Name, space); loaded {
				continue
			}

		case proto.CatalogChangeItem_DeleteSpace:
			spaceItem := new(proto.CatalogChangeSpaceDelete)
			if err := types.UnmarshalAny(routeItem.Item, spaceItem); err != nil {
				return errors.Info(err, "unmarshal delete Space item failed")
			}

			c.spaces.Delete(spaceItem.Sid)
		default:
		}
		c.updateRouteVersion(routeItem.RouteVersion)
	}

	c.updateRouteVersion(routeVersion)
	return nil
}

func (c *Catalog) getRouteVersion() uint64 {
	return atomic.LoadUint64(&c.routeVersion)
}

func (c *Catalog) updateRouteVersion(new uint64) {
	old := atomic.LoadUint64(&c.routeVersion)
	if old < new {
		for {
			// update success, break
			if atomic.CompareAndSwapUint64(&c.routeVersion, old, new) {
				break
			}
			// already update, break
			old = atomic.LoadUint64(&c.routeVersion)
			if old >= new {
				break
			}
			// otherwise, retry cas
		}
	}
}

func (c *Catalog) initDisk(ctx context.Context, nodeID proto.NodeID) {
	span := trace.SpanFromContext(ctx)

	// load disk from master
	registeredDisks, err := c.transport.ListDisks(ctx)
	if err != nil {
		span.Fatalf("list disks from master failed: %s", err)
	}
	registeredDisksMap := make(map[proto.DiskID]proto.Disk)
	// map's key is disk path, and map's value is the disk which has not been repaired yet,
	// disk which not repaired can be replaced for security consider
	registerDiskPathsMap := make(map[string]proto.Disk)
	for _, disk := range registeredDisks {
		registeredDisksMap[disk.DiskID] = disk
		if disk.Status != proto.DiskStatus_DiskStatusRepaired {
			registerDiskPathsMap[disk.Path] = disk
		}
	}

	// load disk from local
	disks := make([]*disk, len(c.cfg.Disks))
	for _, diskPath := range c.cfg.Disks {
		disk := openDisk(ctx, diskConfig{
			nodeID:       nodeID,
			diskPath:     diskPath,
			storeConfig:  c.cfg.StoreConfig,
			raftConfig:   c.cfg.RaftConfig,
			shardHandler: c,
		})
		disks = append(disks, disk)
	}
	// compare local disk and remote disk info, alloc new disk id and register new disk
	// when local disk is not register and local disk is not saved
	newDisks := make([]*disk, 0)
	for _, disk := range disks {
		// not disk id and the old path disk device has been repaired, add new disk
		if disk.DiskID == 0 {
			if unrepairedDisk, ok := registerDiskPathsMap[disk.Path]; ok {
				span.Fatalf("disk device has been replaced but old disk device[%+v] is not repaired", unrepairedDisk)
			}
			newDisks = append(newDisks, disk)
		}
		// alloc disk id already but didn't register yet, add new disk
		if _, ok := registeredDisksMap[disk.DiskID]; !ok {
			newDisks = append(newDisks, disk)
		}
	}

	// register disk
	for _, disk := range newDisks {
		diskInfo := disk.GetDiskInfo()
		// alloc new disk id
		if diskInfo.DiskID == 0 {
			diskID, err := c.transport.AllocDiskID(ctx)
			if err != nil {
				span.Fatalf("alloc disk id failed: %s", err)
			}
			diskInfo.DiskID = diskID
		}
		// save disk meta
		if err := disk.SaveDiskInfo(); err != nil {
			span.Fatalf("save disk info[%+v] failed: %s", diskInfo, err)
		}
		// register disk
		if err := c.transport.RegisterDisk(ctx, proto.Disk{
			DiskID: diskInfo.DiskID,
			NodeID: nodeID,
			Path:   diskInfo.Path,
			Info: proto.DiskReport{
				DiskID: diskInfo.DiskID,
				Used:   diskInfo.Used,
				Total:  diskInfo.Total,
			},
		}); err != nil {
			span.Fatalf("register new disk[%+v] failed: %s", disk, err)
		}
	}

	// load disk concurrently
	wg := sync.WaitGroup{}
	wg.Add(len(disks))
	for i := range disks {
		disk := disks[i]
		go func() {
			defer wg.Done()
			if err := disk.Load(ctx); err != nil {
				span.Fatalf("load disk[%+v] failed", disk)
			}
		}()
	}
	wg.Wait()
}

func (c *Catalog) initRaftConfig() {
	c.cfg.RaftConfig.NodeID = uint64(c.transport.GetMyself().ID)
	c.cfg.RaftConfig.Logger = log.DefaultLogger
	c.cfg.RaftConfig.Resolver = &addressResolver{t: c.transport}
}

func initConfig(cfg *Config) {
	if cfg.ShardBaseConfig.TruncateWalLogInterval <= 0 {
		cfg.ShardBaseConfig.TruncateWalLogInterval = 1 << 16
	}
	if cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightNum <= 0 {
		cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightNum = 64
	}
	if cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightSize <= 0 {
		cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightSize = 1 << 20
	}
	if cfg.ShardBaseConfig.InoAllocRangeStep <= 0 {
		cfg.ShardBaseConfig.InoAllocRangeStep = 1 << 10
	}
	if cfg.NodeConfig.GrpcPort == 0 || cfg.NodeConfig.RaftPort == 0 {
		log.Fatalf("invalid node[%+v] config port", cfg.NodeConfig)
	}

	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, lockCF, dataCF, writeCF)
	cfg.StoreConfig.RaftOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, raftWalCF)
}
