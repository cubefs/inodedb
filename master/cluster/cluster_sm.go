package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/common/raft"
	"github.com/cubefs/inodedb/errors"
)

const (
	RaftOpRegisterNode raft.Op = iota + 1
	RaftOpUnregisterNode
	RaftOpUpdateNode
	RaftOpHeartbeat
)

const (
	module = "cluster"
)

func (c *cluster) Apply(ctx context.Context, ops []raft.Op, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	for i := range ops {
		op := ops[i]
		_, newCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[i].ReqID)
		switch op {
		case RaftOpRegisterNode:
			return c.applyRegister(newCtx, datas[i])
		case RaftOpUnregisterNode:
			return c.applyUnregister(newCtx, datas[i])
		case RaftOpHeartbeat:
			return c.applyHeartbeat(newCtx, datas[i])
		case RaftOpUpdateNode:
			return c.applyUpdate(ctx, datas[i])
		default:
			span.Panicf("unsupported operation type: %d", op)
		}
	}
	return nil
}

func (c *cluster) Flush(ctx context.Context) error {
	return nil
}

func (c *cluster) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}

func (c *cluster) GetModuleName() string {
	return module
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
		return errors.ErrNodeNotExist
	}
	expires := time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second)
	n.HandleHeartbeat(ctx, args.ShardCount, expires)
	return nil
}
