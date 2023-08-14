package cluster

import (
	"context"
	"encoding/binary"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/inodedb/errors"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/inodedb/master/raft"
)

type Cluster interface {
	GetNode(ctx context.Context, nodeId uint32) (*NodeInfo, error)
	Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error)
	Register(ctx context.Context, info *NodeInfo) error
	Unregister(ctx context.Context, nodeID uint32) error
	ListNodeInfo(ctx context.Context, role proto.NodeRole) []*NodeInfo
	HandleHeartbeat(ctx context.Context, nodeId uint32) error
	Load(ctx context.Context) error
}

type cluster struct {
	clusterID uint32
	allocator AllocMgr
	allNodes  sync.Map
	allHosts  sync.Map

	raft  raft.Raft
	store *store.Store

	timeout int

	currentNodeID uint32

	lock sync.RWMutex
}

type Config struct {
	ClusterID         uint32 `json:"cluster_id"`
	DbPath            string `json:"db_path"`
	HeartbeatTimeoutS int    `json:"heartbeat_timeout_s"`
}

func NewCluster(ctx context.Context, cfg *Config) Cluster {
	// open db
	db := store.NewStore(ctx, &store.DBConfig{Path: cfg.DbPath})
	c := &cluster{
		clusterID: cfg.ClusterID,
		store:     db,
	}
	return c
}

func (c *cluster) GetNode(ctx context.Context, nodeId uint32) (*NodeInfo, error) {
	value, ok := c.allNodes.Load(nodeId)
	if !ok {
		return nil, errors.ErrNotFound
	}
	n := value.(*node)
	return n.info, nil
}

func (c *cluster) Alloc(ctx context.Context, args *AllocArgs) ([]*NodeInfo, error) {
	return c.allocator.Alloc(ctx, args)
}

func (c *cluster) Register(ctx context.Context, info *NodeInfo) error {
	span := trace.SpanFromContextSafe(ctx)
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, loaded := c.allHosts.LoadOrStore(info.Addr, nil); loaded {
		return errors.ErrNodeAlreadyExist
	}

	nodeID := c.generateNodeID()
	info.Id = nodeID
	newNode := &node{
		nodeId:           nodeID,
		info:             info,
		heartbeatTimeout: c.timeout,
		expires:          time.Now().Add(time.Duration(c.timeout) * time.Second),
	}
	// todo 1. raft
	if _, loaded := c.allNodes.Load(nodeID); loaded {
		span.Errorf("the node[%d] already exist in cluster, please check raft consistent", nodeID)
		return errors.ErrNodeAlreadyExist
	}
	// 2. persistent
	kvStore := c.store.KVStore()
	data, err := info.Marshal()
	if err != nil {
		return err
	}
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, info.Id)
	if err := kvStore.SetRaw(ctx, nodeCF, key, data, nil); err != nil {
		return err
	}
	c.allNodes.Store(nodeID, newNode)

	if !newNode.contains(ctx, proto.NodeRole_ShardServer) {
		return nil
	}
	c.allocator.Put(ctx, newNode)
	return nil
}

func (c *cluster) Unregister(ctx context.Context, nodeID uint32) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// todo 1. raft

	// 2. persistent
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, nodeID)
	kvStore := c.store.KVStore()
	err := kvStore.Delete(ctx, nodeCF, key, nil)
	if err != nil {
		return err
	}

	if value, loaded := c.allNodes.LoadAndDelete(nodeID); loaded {
		nodeInfo := value.(*NodeInfo)
		c.allocator.Remove(ctx, nodeID, nodeInfo.Az)
	}
	return nil
}

func (c *cluster) ListNodeInfo(ctx context.Context, role proto.NodeRole) []*NodeInfo {
	var res []*NodeInfo
	c.allNodes.Range(func(key, value interface{}) bool {
		n := value.(*node)
		if role == -1 || n.contains(ctx, role) {
			res = append(res, n.info)
		}
		return true
	})
	return res
}

func (c *cluster) HandleHeartbeat(ctx context.Context, nodeId uint32) error {
	value, ok := c.allNodes.Load(nodeId)
	if !ok {
		log.Warnf("receive a heartbeat from a node[%v] of az[%s] not exist in cluster", nodeId)
		return errors.ErrNodeNotExist
	}
	n := value.(*node)
	n.handleHeartbeat(ctx)
	return nil
}

func (c *cluster) Load(ctx context.Context) error {
	// todo read from db
	return nil
}

func (c *cluster) generateNodeID() uint32 {
	return atomic.AddUint32(&c.currentNodeID, 1)
}
