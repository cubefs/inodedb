package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster/client"
	idGenerator "github.com/cubefs/inodedb/master/idgenerator"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
)

type allocatorFunc func(ctx context.Context) Allocator

type Cluster interface {
	GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error)
	Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error)
	GetClient(ctx context.Context, nodeId uint32) (client.MasterClient, error)
	Register(ctx context.Context, args *proto.Node) error
	Unregister(ctx context.Context, nodeID uint32) error
	ListNodeInfo(ctx context.Context, role proto.NodeRole) ([]*proto.Node, error)
	HandleHeartbeat(ctx context.Context, args *HeartbeatArgs) error
	Load(ctx context.Context) error
	Close()
}

type cluster struct {
	clusterId uint32

	allocators sync.Map
	allNodes   sync.Map
	allHosts   sync.Map

	cfg         *Config
	client      client.Client
	raftGroup   raft.Group
	storage     *storage
	idGenerator idGenerator.IDGenerator

	//read only
	azs              map[string]struct{}
	allocatorFuncMap map[proto.NodeRole]allocatorFunc

	done chan struct{}
	lock sync.RWMutex
}

type Config struct {
	ClusterId uint32   `json:"cluster_id"`
	Azs       []string `json:"azs"`

	HeartbeatTimeoutS int `json:"heartbeat_timeout_s"`
	RefreshIntervalS  int `json:"refresh_interval_s"`

	ShardServerConfig client.Config `json:"shard_server_config"`

	Store *store.Store `json:"-"`
}

func NewCluster(ctx context.Context, cfg *Config) Cluster {
	span := trace.SpanFromContext(ctx)

	generator, err := idGenerator.NewIDGenerator(cfg.Store)
	if err != nil {
		span.Fatalf("new id generator failed: %s", err)
	}

	sc, err := client.NewClient(ctx, &cfg.ShardServerConfig)
	if err != nil {
		span.Fatalf("new shard server client failed: %s", err)
	}

	c := &cluster{
		clusterId:        cfg.ClusterId,
		cfg:              cfg,
		idGenerator:      generator,
		client:           sc,
		storage:          &storage{kvStore: cfg.Store.KVStore()},
		done:             make(chan struct{}),
		azs:              make(map[string]struct{}),
		allocatorFuncMap: make(map[proto.NodeRole]allocatorFunc),
	}
	for _, az := range cfg.Azs {
		c.azs[az] = struct{}{}
	}
	c.allocatorFuncMap[proto.NodeRole_ShardServer] = NewShardServerAllocator

	return c
}

func (c *cluster) SetRaftGroup(raftGroup raft.Group) {
	c.raftGroup = raftGroup
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

func (c *cluster) GetClient(ctx context.Context, nodeId uint32) (client.MasterClient, error) {
	_, ok := c.allNodes.Load(nodeId)
	if !ok {
		return nil, errors.ErrNotFound
	}
	return c.client.GetClient(ctx, nodeId)
}

func (c *cluster) Alloc(ctx context.Context, args *AllocArgs) ([]*nodeInfo, error) {
	span := trace.SpanFromContextSafe(ctx)

	if !c.isValidAz(args.Az) {
		span.Warnf("alloc args az[%s] invalid", args.Az)
		return nil, errors.ErrInvalidAz
	}

	if !c.isValidRole(args.Role) {
		span.Warnf("alloc args role[%d] invalid", args.Role)
		return nil, errors.ErrInvalidNodeRole
	}

	value, ok := c.allocators.Load(args.Role)
	if !ok {
		return nil, errors.ErrNodeRoleNotExist
	}
	mgr := value.(Allocator)
	alloc, err := mgr.Alloc(ctx, args)
	if err != nil {
		span.Warnf("alloc failed, err: %s", err)
	}
	return alloc, err
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
		span.Errorf("the node[%s] already exist in cluster", args.Addr)
		return errors.ErrNodeAlreadyExist
	}

	base, _, err := c.idGenerator.Alloc(ctx, idGeneratorName, 1)
	if err != nil {
		span.Errorf("get node[%s] id failed, err: %s", args.Addr, err)
		return err
	}
	nodeID := base
	newInfo := &nodeInfo{}
	newInfo.ToDBNode(args)
	newInfo.Id = uint32(nodeID)

	data, err := newInfo.Marshal()
	if err != nil {
		return err
	}

	_, err = c.raftGroup.Propose(ctx, &raft.ProposeRequest{
		Module:     module,
		Op:         RaftOpRegisterNode,
		Data:       data,
		WithResult: false,
	})
	if err != nil {
		return err
	}
	return c.handleRegister(ctx, data)
}

func (c *cluster) Unregister(ctx context.Context, nodeId uint32) error {
	span := trace.SpanFromContextSafe(ctx)

	if _, hit := c.allNodes.Load(nodeId); !hit {
		span.Errorf("node[%d] not found", nodeId)
		return errors.ErrNodeDoesNotFound
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
		if role == -1 || n.Contains(ctx, role) {
			n.lock.RLock()
			info := n.info.ToProtoNode()
			n.lock.RUnlock()
			res = append(res, info)
		}
		return true
	})
	return res, nil
}

func (c *cluster) HandleHeartbeat(ctx context.Context, args *HeartbeatArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	if _, hit := c.allNodes.Load(args.NodeID); !hit {
		span.Errorf("node[%d] not found", args.NodeID)
		return errors.ErrNotFound
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
	return c.handleHeartbeat(ctx, data)
}

func (c *cluster) Load(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	infos, err := c.storage.Load(ctx)
	if err != nil {
		span.Fatalf("read node data from rocksdb failed, err: %s", err)
	}

	for _, info := range infos {
		newNode := &node{
			info:             info,
			nodeId:           info.Id,
			heartbeatTimeout: c.cfg.HeartbeatTimeoutS,
		}
		c.allNodes.Store(info.Id, newNode)
		c.allHosts.Store(info.Addr, nil)
	}

	c.refresh(ctx)
	return nil
}

func (c *cluster) refresh(ctx context.Context) {
	var allNodes []*node
	c.allNodes.Range(func(key, value interface{}) bool {
		n := value.(*node)
		allNodes = append(allNodes, n)
		return true
	})

	mgrs := make(map[proto.NodeRole]Allocator, len(c.allocatorFuncMap))
	for role, f := range c.allocatorFuncMap {
		mgrs[role] = f(ctx)
	}

	for _, n := range allNodes {
		if !n.IsAvailable() {
			continue
		}

		info := n.GetInfo()
		for _, role := range info.Roles {
			mgrs[role].Put(ctx, n)
		}
	}

	for role, mgr := range mgrs {
		c.allocators.Store(role, mgr)
	}
}

func (c *cluster) Close() {
	close(c.done)
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
