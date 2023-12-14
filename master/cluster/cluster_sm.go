package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/raft"
)

const (
	RaftOpRegisterNode uint32 = iota + 1
	RaftOpUnregisterNode
	RaftOpUpdateNode
	RaftOpHeartbeat
)

const (
	Module = "cluster"
)

func (c *cluster) Apply(cxt context.Context, pd raft.ProposalData, index uint64) (ret interface{}, err error) {
	data := pd.Data
	_, newCtx := trace.StartSpanFromContextWithTraceID(context.Background(), "", string(pd.Context))
	op := pd.Op
	switch op {
	case RaftOpRegisterNode:
		return nil, c.applyRegister(newCtx, data)
	case RaftOpUnregisterNode:
		return nil, c.applyUnregister(newCtx, data)
	case RaftOpHeartbeat:
		return nil, c.applyHeartbeat(newCtx, data)
	case RaftOpUpdateNode:
		return nil, c.applyUpdate(newCtx, data)
	default:
		return nil, err
	}
}

func (c *cluster) Flush(ctx context.Context) error {
	return nil
}

func (c *cluster) LeaderChange(peerID uint64) error {
	return nil
}

func (c *cluster) applyRegister(ctx context.Context, data []byte) error {
	span := trace.SpanFromContextSafe(ctx)

	info := &nodeInfo{}
	err := info.Unmarshal(data)
	if err != nil {
		return err
	}

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	if n := c.allNodes.GetByNameNoLock(info.Addr); n != nil {
		span.Warnf("node[%s] has been exist", info.Addr)
		return nil
	}

	nodeId := info.Id
	newNode := &node{
		nodeId:  nodeId,
		info:    info,
		expires: time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second),
	}
	if err = c.storage.Put(ctx, info); err != nil {
		return err
	}
	c.allNodes.PutNoLock(newNode)
	span.Debugf("register node success, node: %+v", newNode.info)
	return nil
}

func (c *cluster) applyUpdate(ctx context.Context, data []byte) error {
	span := trace.SpanFromContextSafe(ctx)

	info := &nodeInfo{}
	err := info.Unmarshal(data)
	if err != nil {
		return err
	}

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	if n := c.allNodes.GetByNameNoLock(info.Addr); n == nil {
		span.Warnf("node[%s] not exist", info.Addr)
		return nil
	}

	nodeId := info.Id
	newNode := &node{
		nodeId:  nodeId,
		info:    info,
		expires: time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second),
	}
	if err = c.storage.Put(ctx, info); err != nil {
		return err
	}
	c.allNodes.PutNoLock(newNode)
	span.Debugf("update node success, node: %+v", newNode.info)
	return nil
}

func (c *cluster) applyUnregister(ctx context.Context, data []byte) error {
	nodeId := c.decodeNodeId(data)

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	get := c.allNodes.GetNoLock(nodeId)
	if get == nil {
		return nil
	}
	err := c.storage.Delete(ctx, nodeId)
	if err != nil {
		return err
	}
	c.allNodes.DeleteNoLock(nodeId)
	return nil
}

func (c *cluster) applyHeartbeat(ctx context.Context, data []byte) error {
	args := &HeartbeatArgs{}
	err := json.Unmarshal(data, args)
	if err != nil {
		return err
	}

	n := c.allNodes.Get(args.NodeID)
	if n == nil {
		return apierrors.ErrNodeNotExist
	}
	expires := time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second)
	n.HandleHeartbeat(ctx, args.ShardCount, expires)
	return nil
}
