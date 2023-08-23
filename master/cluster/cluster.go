package cluster

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	sclient "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster/client"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
)

type Cluster interface {
	GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
	AddShard(ctx context.Context, args *ShardAddArgs) error
	Register(ctx context.Context, args *proto.Node) error
	Unregister(ctx context.Context, nodeID uint32) error
	ListNodeInfo(ctx context.Context, role proto.NodeRole) ([]*proto.Node, error)
	HandleHeartbeat(ctx context.Context, nodeId uint32) error
	Load(ctx context.Context) error
	Close()
}

type cluster struct {
	clusterID     uint32
	currentNodeID uint32

	allocator sync.Map
	allNodes  sync.Map
	allHosts  sync.Map

	cfg       *Config
	client    client.Client
	raftGroup raft.Group
	store     *store.Store

	nodeRoles map[proto.NodeRole]struct{}
	azs       map[string]struct{}

	closeChan chan struct{}

	lock sync.RWMutex
}

type Config struct {
	ClusterID uint32   `json:"cluster_id"`
	Azs       []string `json:"azs"`
	DBPath    string   `json:"db_path"`

	NodeMaxLoad       int32 `json:"node_max_load"`
	HeartbeatTimeoutS int   `json:"heartbeat_timeout_s"`
	RefreshIntervalS  int   `json:"refresh_interval_s"`

	AllocConfig
	sclient.ShardServerConfig
}

func NewCluster(ctx context.Context, cfg *Config) Cluster {
	// open db
	span := trace.SpanFromContext(ctx)
	db, err := store.NewStore(ctx, &store.DBConfig{Path: cfg.DBPath})
	if err != nil {
		span.Fatalf("new store instance failed: %s", err)
	}
	c := &cluster{
		clusterID: cfg.ClusterID,
		store:     db,
		cfg:       cfg,
		closeChan: make(chan struct{}),
		azs:       make(map[string]struct{}),
	}
	for _, az := range cfg.Azs {
		c.azs[az] = struct{}{}
	}
	return c
}

func (c *cluster) GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error) {
	value, ok := c.allNodes.Load(nodeId)
	if !ok {
		return nil, errors.ErrNotFound
	}
	n := value.(*node)
	n.lock.RLock()
	info := n.info.ToProtoNode()
	n.lock.RUnlock()

	return info, nil
}

func (c *cluster) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if !c.isValidAz(args.Az) {
		span.Warnf("alloc args az[%s] invalid", args.Az)
		return nil, errors.ErrInvalidAz
	}

	if !c.isValidRole(args.Role) {
		span.Warnf("alloc args role[%d] invalid", args.Role)
		return nil, errors.ErrInvalidNodeRole
	}

	value, ok := c.allocator.Load(args.Role)
	if !ok {
		return nil, errors.ErrNodeRoleNotExist
	}
	mgr := value.(AllocMgr)
	alloc, err := mgr.Alloc(ctx, args)
	if err != nil {
		span.Warnf("alloc failed, err: %s", err)
	}
	return alloc, err
}

func (c *cluster) AddShard(ctx context.Context, args *ShardAddArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	value, ok := c.allNodes.Load(args.NodeId)
	if !ok {
		span.Errorf("node[%d] not found", args.NodeId)
		return errors.ErrNotFound
	}
	newNode := value.(*node)
	if !newNode.contains(ctx, proto.NodeRole_ShardServer) {
		span.Errorf("the node[%d] not shard server, can not add shard", args.NodeId)
		return errors.ErrInvalidNodeRole
	}

	err := c.client.AddShard(ctx, args)
	if err != nil {
		span.Errorf("add shard failed at node[%d], err: %s", args.NodeId, err)
		return err
	}
	return nil
}

func (c *cluster) Register(ctx context.Context, args *proto.Node) error {
	span := trace.SpanFromContextSafe(ctx)

	if !c.isValidAz(args.Az) {
		span.Warnf("register node az[%s] not match", args.Az)
		return errors.ErrInvalidAz
	}
	for _, role := range args.Roles {
		if !c.isValidRole(role) {
			span.Warnf("register node role[%d] not match", role)
			return errors.ErrInvalidNodeRole
		}
	}
	if _, loaded := c.allHosts.LoadOrStore(args.Addr, nil); loaded {
		span.Errorf("the node[%s] already exist in cluster, please check raft consistent", args.Addr)
		return errors.ErrNodeAlreadyExist
	}

	nodeID := c.generateNodeID()
	newInfo := &NodeInfo{}
	newInfo.ToDBNode(args)
	newInfo.Id = nodeID

	data, err := newInfo.Marshal()
	if err != nil {
		return err
	}
	// raft propose
	return c.handleRegister(ctx, data)
}

func (c *cluster) Unregister(ctx context.Context, nodeId uint32) error {
	span := trace.SpanFromContextSafe(ctx)

	if _, hit := c.allNodes.Load(nodeId); !hit {
		span.Errorf("node[%d] not found", nodeId)
		return errors.ErrNotFound
	}
	data := c.encodeNodeId(nodeId)
	// todo 1. raft
	return c.handleUnregister(ctx, data)
}

func (c *cluster) ListNodeInfo(ctx context.Context, role proto.NodeRole) ([]*proto.Node, error) {
	span := trace.SpanFromContextSafe(ctx)
	if !c.isValidRole(role) {
		span.Warnf("list node role[%d] invalid", role)
		return nil, errors.ErrInvalidNodeRole
	}
	var res []*proto.Node
	c.allNodes.Range(func(key, value interface{}) bool {
		n := value.(*node)
		if role == -1 || n.contains(ctx, role) {
			n.lock.RLock()
			info := n.info.ToProtoNode()
			n.lock.RUnlock()
			res = append(res, info)
		}
		return true
	})
	return res, nil
}

func (c *cluster) HandleHeartbeat(ctx context.Context, nodeId uint32) error {
	span := trace.SpanFromContextSafe(ctx)

	if _, hit := c.allNodes.Load(nodeId); !hit {
		span.Errorf("node[%d] not found", nodeId)
		return errors.ErrNotFound
	}

	data := c.encodeNodeId(nodeId)

	// todo raft propose
	return c.handleHeartbeat(ctx, data)
}

func (c *cluster) refresh(ctx context.Context) {
	var allNodes []*node
	c.allNodes.Range(func(key, value interface{}) bool {
		n := value.(*node)
		allNodes = append(allNodes, n)
		return true
	})

	c.lock.RLock()
	mgrs := make(map[proto.NodeRole]AllocMgr, len(c.nodeRoles))
	for role := range c.nodeRoles {
		mgrs[role] = NewAllocMgr(ctx, &AllocConfig{NodeLoadThreshold: c.cfg.NodeLoadThreshold})
	}
	c.lock.RUnlock()

	for _, n := range allNodes {
		if !n.isAvailable() {
			continue
		}
		n.lock.RLock()
		az := n.info.Az
		roles := make([]proto.NodeRole, len(n.info.Roles))
		copy(roles, n.info.Roles)
		n.lock.RUnlock()

		for _, role := range roles {
			mgrs[role].Put(ctx, az, n)
		}
	}

	for role, mgr := range mgrs {
		c.allocator.Store(role, mgr)
	}
}

func (c *cluster) Load(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	kvStore := c.store.KVStore()
	list := kvStore.List(ctx, nodeCF, nil, nil, nil)

	key, value, err := list.ReadNext()
	if err != nil {
		span.Errorf("read node data from rocksdb failed, err: %s", err)
		return err
	}
	maxId := uint32(0)
	for key != nil {
		data := value.Value()
		info := &NodeInfo{}
		err = info.Unmarshal(data)
		if err != nil {
			span.Errorf("unmarshal data from rocksdb to node info failed, err: %s", err)
			return err
		}
		newNode := &node{
			info:             info,
			nodeId:           info.Id,
			load:             c.cfg.NodeMaxLoad,
			heartbeatTimeout: c.cfg.HeartbeatTimeoutS,
		}
		c.allNodes.Store(info.Id, newNode)
		if maxId < info.Id {
			maxId = info.Id
		}
		key, value, err = list.ReadNext()
		if err != nil {
			span.Errorf("read node data from rocksdb failed, err: %s", err)
			return err
		}
	}
	atomic.StoreUint32(&c.currentNodeID, maxId)

	c.refresh(ctx)
	return nil
}

func (c *cluster) Close() {
	close(c.closeChan)
}

func (c *cluster) generateNodeID() uint32 {
	return atomic.AddUint32(&c.currentNodeID, 1)
}

func (c *cluster) loop() {
	_, ctxNew := trace.StartSpanFromContext(context.Background(), "")
	ticker := time.NewTicker(time.Duration(c.cfg.RefreshIntervalS) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.refresh(ctxNew)
			case <-c.closeChan:
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
