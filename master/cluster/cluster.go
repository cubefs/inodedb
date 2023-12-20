package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster/transport"
	idGenerator "github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
)

const (
	defaultSplitMapNum       = 16
	defaultRefreshIntervalS  = 5
	defaultHeartbeatTimeoutS = 30
)

type Cluster interface {
	GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error)
	Alloc(ctx context.Context, args *AllocArgs) ([]*proto.Disk, error)
	GetClient(ctx context.Context, nodeId uint32) (transport.ShardServerClient, error)
	Register(ctx context.Context, args *proto.Node) (uint32, error)
	Unregister(ctx context.Context, nodeID uint32) error
	ListNodeInfo(ctx context.Context, roles []proto.NodeRole) ([]proto.Node, error)
	HandleHeartbeat(ctx context.Context, args *HeartbeatArgs) error
	AllocDiskID(ctx context.Context) (uint32, error)
	AddDisk(ctx context.Context, args *proto.Disk) error
	ListDisk(ctx context.Context, args *proto.ListDiskRequest) ([]proto.Disk, uint32, error)
	GetDisk(ctx context.Context, id uint32) (proto.Disk, error)
	SetBroken(ctx context.Context, diskId uint32) error
	Load(ctx context.Context)
	GetSM() raft.Applier
	SetRaftGroup(raftGroup raft.Group)
	Close()
}

type Config struct {
	ClusterId         uint32            `json:"cluster_id"`
	Azs               []string          `json:"azs"`
	GrpcPort          uint32            `json:"grpc_port"`
	HttpPort          uint32            `json:"http_port"`
	HeartbeatTimeoutS int               `json:"heartbeat_timeout_s"`
	RefreshIntervalS  int               `json:"refresh_interval_s"`
	ShardServerConfig *transport.Config `json:"shard_server_config"`

	Store       *store.Store            `json:"-"`
	IdGenerator idGenerator.IDGenerator `json:"-"`
	RaftGroup   raft.Group              `json:"-"`
}

type allocatorFunc func(ctx context.Context) Allocator

type cluster struct {
	clusterId uint32

	allocators sync.Map
	allNodes   *concurrentNodes
	disks      *diskMgr

	cfg         *Config
	transporter transport.Transport
	raftGroup   raft.Group
	storage     *storage
	idGenerator idGenerator.IDGenerator

	// read only
	azs              map[string]struct{}
	allocatorFuncMap map[proto.NodeRole]allocatorFunc

	done chan struct{}
	lock sync.RWMutex
}

func NewCluster(ctx context.Context, cfg *Config) Cluster {
	err := initConfig(cfg)
	if err != nil {
		log.Fatalf("init config failed: err: %s", err)
	}

	c := &cluster{
		clusterId:        cfg.ClusterId,
		cfg:              cfg,
		idGenerator:      cfg.IdGenerator,
		raftGroup:        cfg.RaftGroup,
		allNodes:         newConcurrentNodes(defaultSplitMapNum),
		storage:          &storage{kvStore: cfg.Store.KVStore()},
		done:             make(chan struct{}),
		azs:              make(map[string]struct{}),
		allocatorFuncMap: make(map[proto.NodeRole]allocatorFunc),
		disks:            &diskMgr{disks: map[uint32]*diskInfo{}},
	}
	for _, az := range cfg.Azs {
		c.azs[az] = struct{}{}
	}
	c.allocatorFuncMap[proto.NodeRole_ShardServer] = NewShardServerAllocator
	c.Load(ctx)
	c.loop()

	return c
}

func (c *cluster) GetSM() raft.Applier {
	return c
}

func (c *cluster) SetRaftGroup(raftGroup raft.Group) {
	c.raftGroup = raftGroup
}

func (c *cluster) GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error) {
	n := c.allNodes.Get(nodeId)
	if n == nil {
		return nil, apierrors.ErrNotFound
	}
	n.lock.RLock()
	info := n.info.ToProtoNode()
	n.lock.RUnlock()

	return info, nil
}

func (c *cluster) GetClient(ctx context.Context, nodeId uint32) (transport.ShardServerClient, error) {
	if c.transporter == nil {
		c.lock.Lock()
		if c.transporter == nil {
			if c.cfg.ShardServerConfig == nil {
				c.cfg.ShardServerConfig = &transport.Config{}
			}
			c.cfg.ShardServerConfig.GrpcPort = c.cfg.GrpcPort
			transporter, err := transport.NewTransport(ctx, c.cfg.ShardServerConfig)
			if err != nil {
				c.lock.Unlock()
				return nil, errors.Info(err, "new shard server transporter failed: %s")
			}
			c.transporter = transporter
		}
		c.lock.Unlock()
	}
	// n := c.allNodes.Get(nodeId)
	// if n == nil {
	// 	return nil, apierrors.ErrNotFound
	// }
	return c.transporter.GetShardServerClient(ctx, nodeId)
}

func (c *cluster) Alloc(ctx context.Context, args *AllocArgs) ([]*proto.Disk, error) {
	span := trace.SpanFromContextSafe(ctx)

	if !c.isValidAz(args.AZ) {
		span.Warnf("alloc args az[%s] invalid", args.AZ)
		return nil, apierrors.ErrInvalidAz
	}

	if !c.isValidRole(args.Role) {
		span.Warnf("alloc args role[%d] invalid", args.Role)
		return nil, apierrors.ErrInvalidNodeRole
	}

	value, ok := c.allocators.Load(args.Role)
	if !ok {
		return nil, apierrors.ErrNodeRoleNotExist
	}
	mgr := value.(Allocator)
	allocRet, err := mgr.Alloc(ctx, args)
	if err != nil {
		span.Warnf("alloc failed, err: %s", err)
		return nil, err
	}

	if len(allocRet) < args.Count {
		span.Warnf("alloc result count[%d] less than requested count[%d]", len(allocRet), args.Count)
		return nil, apierrors.ErrNoAvailableNode
	}

	return allocRet, err
}

func (c *cluster) AllocDiskID(ctx context.Context) (uint32, error) {
	span := trace.SpanFromContext(ctx)
	span.Debugf("alloc disk id")

	_, id, err := c.idGenerator.Alloc(ctx, diskIdName, 1)
	if err != nil {
		span.Errorf("all diskId failed, err: %s", err.Error())
		return 0, err
	}

	span.Debugf("alloc disk id success, disk id: %d", id)
	return uint32(id), nil
}

func (c *cluster) Register(ctx context.Context, args *proto.Node) (uint32, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("register node, args : %+v", args)
	if !c.isValidAz(args.Az) {
		span.Warnf("register node az[%s] not match", args.Az)
		return 0, apierrors.ErrInvalidAz
	}

	if len(args.Roles) == 0 {
		return 0, apierrors.ErrInvalidNodeRole
	}

	for _, role := range args.Roles {
		if !c.isValidRole(role) {
			span.Warnf("register node role[%d] not match", role)
			return 0, apierrors.ErrInvalidNodeRole
		}
	}

	get := c.allNodes.GetByName(args.Addr)
	if get != nil {
		if get.GetInfo().Compare(args) {
			span.Warnf("the node[%s] already exist in cluster", args.Addr)
			return get.nodeId, nil
		}

		// only support update roles
		info := get.GetInfo()
		info.UpdateRoles(args.Roles)
		data, err := info.Marshal()
		if err != nil {
			return 0, err
		}
		_, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
			Module: []byte(Module),
			Op:     RaftOpUpdateNode,
			Data:   data,
		})
		if err != nil {
			span.Errorf("update node info failed, err %v", err.Error())
			return 0, err
		}
		return info.Id, nil
	}

	_, id, err := c.idGenerator.Alloc(ctx, nodeIdName, 1)
	if err != nil {
		span.Errorf("get node[%s] id failed, err: %s", args.Addr, err)
		return 0, err
	}
	nodeID := uint32(id)
	newInfo := &nodeInfo{}
	newInfo.ToDBNode(args)
	newInfo.Id = nodeID

	data, err := newInfo.Marshal()
	if err != nil {
		return 0, err
	}

	_, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpRegisterNode,
		Data:   data,
	})
	if err != nil {
		span.Errorf("register node failed, err %v", err)
		return 0, err
	}
	return nodeID, nil
}

func (c *cluster) Unregister(ctx context.Context, nodeId uint32) error {
	span := trace.SpanFromContextSafe(ctx)

	n := c.allNodes.Get(nodeId)
	if n == nil {
		span.Errorf("node[%d] not found", nodeId)
		return apierrors.ErrNodeDoesNotFound
	}
	data := c.encodeNodeId(nodeId)
	// todo 1. raft

	_, err := c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpUnregisterNode,
		Data:   data,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *cluster) ListNodeInfo(ctx context.Context, roles []proto.NodeRole) ([]proto.Node, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive ListNodeInfo request, args: %v", roles)
	var res []proto.Node
	for _, role := range roles {
		if !c.isValidRole(role) {
			span.Warnf("list node role[%d] invalid", role)
			return nil, apierrors.ErrInvalidNodeRole
		}
		c.allNodes.Range(func(n *node) error {
			if n.Contains(ctx, role) {
				n.lock.RLock()
				info := n.info.ToProtoNode()
				n.lock.RUnlock()
				res = append(res, *info)
			}
			return nil
		})
	}
	return res, nil
}

func (c *cluster) HandleHeartbeat(ctx context.Context, args *HeartbeatArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive HeartBeat, args: %v", args)
	n := c.allNodes.Get(args.NodeID)
	if n == nil {
		span.Errorf("node[%d] not found", args.NodeID)
		return apierrors.ErrNodeDoesNotFound
	}
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	_, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpHeartbeat,
		Data:   data,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *cluster) AddDisk(ctx context.Context, args *proto.Disk) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive AddDisk, args: %v", args)
	if args.DiskID == 0 || args.NodeID == 0 {
		span.Warnf("disk args is not valid, args %+v", args)
		return apierrors.ErrInvalidData
	}

	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	_, err = c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpAddDisk,
		Data:   data,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *cluster) SetBroken(ctx context.Context, diskId uint32) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive AddDisk, args: %d", diskId)
	data := c.encodeDiskId(diskId)
	_, err := c.raftGroup.Propose(ctx, &raft.ProposalData{
		Module: []byte(Module),
		Op:     RaftOpSetBroken,
		Data:   data,
	})
	if err != nil {
		return err
	}
	span.Debugf("set disk[%d] broken success", diskId)
	return nil
}

func (c *cluster) ListDisk(ctx context.Context, args *proto.ListDiskRequest) ([]proto.Disk, uint32, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive ListDisk, args: %v", args)

	rets := make([]proto.Disk, 0)
	disks := c.disks.getSortedDisks()
	cnt := 0

	match := func(disk *diskInfo) bool {
		ifo := disk.node.info
		if args.Az != "" && ifo.Az != args.Az {
			return false
		}
		if args.NodeID != 0 && ifo.Id != args.NodeID {
			return false
		}
		if args.Rack != "" && ifo.Rack != args.Rack {
			return false
		}
		if args.Status != proto.DiskStatus_DiskStatusUnknown && disk.disk.Status != args.Status {
			return false
		}
		return true
	}

	newMarker := args.Marker
	for _, d := range disks {
		if d.disk.DiskID <= args.Marker {
			continue
		}

		if !match(d) {
			continue
		}

		rets = append(rets, *d.disk)
		cnt++
		newMarker = d.disk.DiskID
		if cnt >= int(args.Count) {
			break
		}
	}

	span.Debugf("ListDisk return disks: %v, marker: %v", rets, newMarker)
	return rets, newMarker, nil
}

func (c *cluster) GetDisk(ctx context.Context, id uint32) (proto.Disk, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive GetDisk, id: %v", id)
	disk := c.disks.get(id)
	if disk == nil {
		return proto.Disk{}, apierrors.ErrDiskNotExist
	}
	return *disk.disk, nil
}

func (c *cluster) Load(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	infos, err := c.storage.Load(ctx)
	if err != nil {
		span.Fatalf("read node data from rocksdb failed, err: %s", err)
	}

	for _, info := range infos {
		newNode := &node{
			info:   info,
			nodeId: info.Id,
		}
		c.allNodes.PutNoLock(newNode)
	}

	disks, err := c.storage.LoadDisk(ctx)
	if err != nil {
		span.Fatalf("read disk data from rocksdb failed, err: %s", err)
	}

	for _, d := range disks {
		node := c.allNodes.GetNoLock(d.NodeID)
		if node == nil {
			span.Fatalf("node[%d] not found, disk %v", d.NodeID, d)
		}

		ifo := &diskInfo{
			node: node,
			disk: d,
		}
		c.disks.addDiskNoLock(d.DiskID, ifo)
	}

	c.refresh(ctx)
}

func (c *cluster) Close() {
	close(c.done)
}

func (c *cluster) refresh(ctx context.Context) {
	allNodes := c.disks.getSortedDisks()
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("refresh allocator nodes: %+v", len(allNodes))

	mgrs := make(map[proto.NodeRole]Allocator, len(c.allocatorFuncMap))
	for role, f := range c.allocatorFuncMap {
		mgrs[role] = f(ctx)
	}

	for _, n := range allNodes {
		if !n.CanAlloc() {
			span.Infof("disk node can't used to alloc shard, disk %+v", n.disk)
			continue
		}

		info := n.node.info
		for _, role := range info.Roles {
			if _, ok := c.allocatorFuncMap[role]; ok {
				mgrs[role].Put(ctx, n)
			}
		}
	}

	for role, mgr := range mgrs {
		c.allocators.Store(role, mgr)
	}
	span.Infof("refresh allocator done")
}

func (c *cluster) loop() {
	_, ctxNew := trace.StartSpanFromContext(context.Background(), "")

	go func() {
		ticker := time.NewTicker(time.Duration(c.cfg.RefreshIntervalS) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.refresh(ctxNew)
			case <-c.done:
				return
			}
		}
	}()
}

func (c *cluster) encodeNodeId(nodeId uint32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, nodeId)
	return key
}

func (c *cluster) decodeNodeId(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func (c *cluster) encodeDiskId(diskId uint32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, diskId)
	return key
}

func (c *cluster) decodeDiskId(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func (c *cluster) isValidRole(role proto.NodeRole) bool {
	return role <= proto.NodeRole_Router && role >= proto.NodeRole_Single
}

func (c *cluster) isValidAz(az string) bool {
	_, ok := c.azs[az]
	return ok
}

func initConfig(cfg *Config) error {
	if len(cfg.Azs) == 0 {
		return errors.New("at least config one az for cluster")
	}
	if cfg.GrpcPort == 0 && cfg.HttpPort == 0 {
		return errors.New("need config http port or grpc port")
	}
	if cfg.RefreshIntervalS <= 0 {
		cfg.RefreshIntervalS = defaultRefreshIntervalS
	}
	if cfg.HeartbeatTimeoutS <= 0 {
		cfg.HeartbeatTimeoutS = defaultHeartbeatTimeoutS
	}
	return nil
}
