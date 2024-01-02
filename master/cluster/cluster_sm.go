package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
)

const (
	RaftOpRegisterNode uint32 = iota + 1
	RaftOpUnregisterNode
	RaftOpUpdateNode
	RaftOpHeartbeat
	RaftOpAddDisk
	RaftOpSetBroken
)

var Module = []byte("cluster")

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
	case RaftOpAddDisk:
		return nil, c.applyAddDisk(newCtx, data)
	case RaftOpSetBroken:
		return nil, c.applySetDiskBroken(newCtx, data)
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

func (c *cluster) applyAddDisk(ctx context.Context, data []byte) error {
	span := trace.SpanFromContextSafe(ctx)
	d := &proto.Disk{}
	err := json.Unmarshal(data, d)
	if err != nil {
		span.Errorf("unmarshal disk failed, err: %v", err)
		return err
	}

	ifo := c.disks.get(d.DiskID)
	if ifo != nil {
		span.Errorf("disk %d already exist", d.DiskID)
		return fmt.Errorf("disk %d already exist", d.DiskID)
	}

	node := c.allNodes.Get(d.NodeID)
	if node == nil {
		span.Errorf("node %d not found", d.NodeID)
		return apierrors.ErrNodeNotExist
	}

	info := protoDiskToInternalDisk(d)
	info.Status = proto.DiskStatus_DiskStatusNormal
	err = c.storage.PutDisk(ctx, info)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", d, err)
		return err
	}

	disk := &disk{
		info: info,
		node: node.info,
	}

	c.disks.addDisk(d.DiskID, disk)
	node.dm.addDiskNoLock(d.DiskID, disk)

	span.Infof("add disk %v success", d)
	return nil
}

func (c *cluster) applySetDiskBroken(ctx context.Context, data []byte) error {
	span := trace.SpanFromContextSafe(ctx)
	diskID := c.decodeDiskId(data)

	disk := c.disks.get(diskID)
	if disk == nil {
		span.Errorf("disk %d not found", diskID)
		return apierrors.ErrDiskNotExist
	}

	oldStatus := disk.info.Status
	disk.info.Status = proto.DiskStatus_DiskStatusBroken
	err := c.storage.PutDisk(ctx, disk.info)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", disk, err)
		disk.info.Status = oldStatus
		return err
	}

	span.Infof("set disk %v broken success", disk.info.DiskID)
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
		span.Warnf("node[%s] has been exist, n %+v", info.Addr, n.GetInfo())
		return apierrors.ErrNodeAlreadyExist
	}

	info.State = proto.NodeState_Alive
	newNode := newNode(info, nil, c.cfg.HeartbeatTimeoutS)
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

	n := c.allNodes.GetByNameNoLock(info.Addr)
	if n == nil {
		span.Warnf("node[%s] not exist", info.Addr)
		return apierrors.ErrNodeNotExist
	}

	newInfo := n.GetInfo()
	newInfo.Roles = info.Roles

	span.Infof("disk info on node, nodeId %d, disk %v", info.ID, n.dm.disks)
	if err = c.storage.Put(ctx, newInfo); err != nil {
		return err
	}

	newNode := newNode(newInfo, n.dm, c.cfg.HeartbeatTimeoutS)
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
	n.HandleHeartbeat(ctx, args.Disks, expires)
	return nil
}
