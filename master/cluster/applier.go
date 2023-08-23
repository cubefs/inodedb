package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/raft"
)

const (
	RaftOpRegisterNode raft.RaftOp = iota + 1
	RaftOpUnregisterNode
	RaftOpHeartbeat
)

func (c *cluster) Apply(ctx context.Context, op raft.RaftOp, data []byte) error {
	switch op {
	case RaftOpRegisterNode:
		return c.handleRegister(ctx, data)
	case RaftOpUnregisterNode:
		return c.handleUnregister(ctx, data)
	case RaftOpHeartbeat:
		return c.handleHeartbeat(ctx, data)
	default:
		panic(fmt.Sprintf("unsupported operation type: %d", op))
	}
	return nil
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

	kvStore := c.store.KVStore()
	key := c.encodeNodeId(nodeId)
	if err = kvStore.SetRaw(ctx, nodeCF, key, data, nil); err != nil {
		return err
	}

	c.allNodes.Store(nodeId, newNode)
	c.allHosts.Store(info.Addr, nil)

	return nil
}

func (c *cluster) handleUnregister(ctx context.Context, data []byte) error {
	nodeId := c.decodeNodeId(data)
	if _, hit := c.allNodes.Load(nodeId); !hit {
		return nil
	}

	key := c.encodeNodeId(nodeId)
	kvStore := c.store.KVStore()
	err := kvStore.Delete(ctx, nodeCF, key, nil)
	if err != nil {
		return err
	}

	c.allNodes.Delete(nodeId)
	return nil
}

func (c *cluster) handleHeartbeat(ctx context.Context, data []byte) error {
	nodeId := c.decodeNodeId(data)
	value, hit := c.allNodes.Load(nodeId)
	if !hit {
		return errors.ErrNodeNotExist
	}

	n := value.(*node)
	n.handleHeartbeat(ctx)
	return nil
}
