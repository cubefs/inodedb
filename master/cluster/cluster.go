package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/common/raft"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster/client"
	idGenerator "github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
)

const (
	defaultSplitMapNum      = 16
	defaultRefreshIntervalS = 5
)

type Cluster interface {
	GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error)
	Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error)
	GetClient(ctx context.Context, nodeId uint32) (client.ShardServerClient, error)
	Register(ctx context.Context, args *proto.Node) (uint32, error)
	Unregister(ctx context.Context, nodeID uint32) error
	ListNodeInfo(ctx context.Context, roles []proto.NodeRole) ([]*proto.Node, error)
	HandleHeartbeat(ctx context.Context, args *HeartbeatArgs) error
	Load(ctx context.Context)
	Close()
}

type Config struct {
	ClusterId         uint32         `json:"cluster_id"`
	Azs               []string       `json:"azs"`
	GrpcPort          uint32         `json:"grpc_port"`
	HttpPort          uint32         `json:"http_port"`
	HeartbeatTimeoutS int            `json:"heartbeat_timeout_s"`
	RefreshIntervalS  int            `json:"refresh_interval_s"`
	ShardServerConfig *client.Config `json:"shard_server_config"`

	Store       *store.Store            `json:"-"`
	IdGenerator idGenerator.IDGenerator `json:"-"`
	RaftGroup   raft.Group              `json:"-"`
}

type allocatorFunc func(ctx context.Context) Allocator

type cluster struct {
	clusterId uint32

	allocators sync.Map
	allNodes   *concurrentNodes

	cfg         *Config
	transporter client.Transporter
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
	if cfg.RefreshIntervalS == 0 {
		cfg.RefreshIntervalS = defaultRefreshIntervalS
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
	}
	for _, az := range cfg.Azs {
		c.azs[az] = struct{}{}
	}
	c.allocatorFuncMap[proto.NodeRole_ShardServer] = NewShardServerAllocator
	c.Load(ctx)
	c.loop()

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

func (c *cluster) GetClient(ctx context.Context, nodeId uint32) (client.ShardServerClient, error) {
	if c.transporter == nil {
		c.lock.Lock()
		if c.transporter == nil {
			if c.cfg.ShardServerConfig == nil {
				c.cfg.ShardServerConfig = &client.Config{}
			}
			c.cfg.ShardServerConfig.GrpcPort = c.cfg.GrpcPort
			transporter, err := client.NewTransporter(ctx, c.cfg.ShardServerConfig)
			if err != nil {
				c.lock.Unlock()
				return nil, errors.Info(err, "new shard server transporter failed: %s")
			}
			c.transporter = transporter
		}
		c.lock.Unlock()
	}
	n := c.allNodes.Get(nodeId)
	if n == nil {
		return nil, apierrors.ErrNotFound
	}
	return c.transporter.GetShardServerClient(ctx, nodeId)
}

func (c *cluster) Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error) {
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
	alloc, err := mgr.Alloc(ctx, args)
	if err != nil {
		span.Warnf("alloc failed, err: %s", err)
	}
	return alloc, err
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
	if get != nil && get.GetInfo().CompareRoles(args.Roles) {
		span.Warnf("the node[%s] already exist in cluster", args.Addr)
		return get.nodeId, nil
	}

	if get != nil {
		info := get.GetInfo()
		info.UpdateRoles(args.Roles)
		data, err := info.Marshal()
		if err != nil {
			return 0, err
		}
		_, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
			Module:     module,
			Op:         RaftOpUpdateNode,
			Data:       data,
			WithResult: false,
		})
		if err != nil {
			return 0, err
		}
		return info.Id, c.applyUpdate(ctx, data)
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

	_, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpRegisterNode,
		Data:       data,
		WithResult: false,
	})
	if err != nil {
		return 0, err
	}
	return nodeID, c.applyRegister(ctx, data)
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

	_, err := c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpUnregisterNode,
		Data:       data,
		WithResult: false,
	})
	if err != nil {
		return err
	}
	return c.applyUnregister(ctx, data)
}

func (c *cluster) ListNodeInfo(ctx context.Context, roles []proto.NodeRole) ([]*proto.Node, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("receive ListNodeInfo request, args: %v", roles)
	var res []*proto.Node
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
				res = append(res, info)
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
	_, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpHeartbeat,
		Data:       data,
		WithResult: false,
	})
	if err != nil {
		return err
	}
	// todo raft propose
	return c.applyHeartbeat(ctx, data)
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

	c.refresh(ctx)
}

func (c *cluster) Close() {
	close(c.done)
}

func (c *cluster) refresh(ctx context.Context) {
	var allNodes []*node
	c.allNodes.Range(func(n *node) error {
		allNodes = append(allNodes, n)
		return nil
	})

	span := trace.SpanFromContextSafe(ctx)
	span.Infof("refresh allocator nodes: %+v", allNodes)

	mgrs := make(map[proto.NodeRole]Allocator, len(c.allocatorFuncMap))
	for role, f := range c.allocatorFuncMap {
		mgrs[role] = f(ctx)
	}

	for _, n := range allNodes {
		if !n.IsAvailable() {
			span.Debugf("node not available, node: %#v, time: %s", n.info, n.expires.String())
			continue
		}

		info := n.GetInfo()
		for _, role := range info.Roles {
			if _, ok := c.allocatorFuncMap[role]; ok {
				mgrs[role].Put(ctx, n)
			}
		}
	}

	for role, mgr := range mgrs {
		c.allocators.Store(role, mgr)
	}
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

func (c *cluster) isValidRole(role proto.NodeRole) bool {
	return role <= proto.NodeRole_Router && role >= proto.NodeRole_Single
}

func (c *cluster) isValidAz(az string) bool {
	_, ok := c.azs[az]
	return ok
}
