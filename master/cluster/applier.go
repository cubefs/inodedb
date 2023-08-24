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
			return c.handleRegister(newCtx, datas[i])
		case RaftOpUnregisterNode:
			return c.handleUnregister(newCtx, datas[i])
		case RaftOpHeartbeat:
			return c.handleHeartbeat(newCtx, datas[i])
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

func (c *cluster) handleRegister(ctx context.Context, data []byte) error {
	info := &NodeInfo{}
	err := info.Unmarshal(data)
	if err != nil {
		return err
	}

	nodeId := info.Id
	newNode := &node{
		nodeId:           nodeId,
		info:             info,
		heartbeatTimeout: c.cfg.HeartbeatTimeoutS,
		expires:          time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second),
	}

	if err = c.store.Put(ctx, info); err != nil {
		return err
	}

	c.allNodes.Store(nodeId, newNode)
	c.allHosts.Store(info.Addr, nil)

	return nil
}

func (c *cluster) handleUnregister(ctx context.Context, data []byte) error {
	nodeId := c.decodeNodeId(data)

	value, hit := c.allNodes.Load(nodeId)
	if !hit {
		return nil
	}

	newNode := value.(*node)
	newNode.lock.RLock()
	addr := newNode.info.Addr
	newNode.lock.RUnlock()

	err := c.store.Delete(ctx, nodeId)
	if err != nil {
		return err
	}
	c.allNodes.Delete(nodeId)
	c.allHosts.Delete(addr)
	return nil
}

func (c *cluster) handleHeartbeat(ctx context.Context, data []byte) error {
	args := &HeartbeatArgs{}
	err := json.Unmarshal(data, args)
	if err != nil {
		return err
	}

	value, hit := c.allNodes.Load(args.NodeID)
	if !hit {
		return errors.ErrNodeNotExist
	}

	n := value.(*node)
	n.handleHeartbeat(ctx, args.ShardCount)
	return nil
}
