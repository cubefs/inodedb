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
	disk := &proto.Disk{}
	err := json.Unmarshal(data, disk)
	if err != nil {
		span.Errorf("unmarshal disk failed, err: %v", err)
		return err
	}

	ifo := c.disks.get(disk.DiskID)
	if ifo != nil {
		span.Errorf("disk %d already exist", disk.DiskID)
		return fmt.Errorf("disk %d already exist", disk.DiskID)
	}

	node := c.allNodes.Get(disk.NodeID)
	if node == nil {
		span.Errorf("node %d not found", disk.NodeID)
		return apierrors.ErrNodeNotExist
	}

	err = c.storage.PutDisk(ctx, disk)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", disk, err)
		return err
	}

	disk.Status = proto.DiskStatus_DiskStatusNormal
	diskInfo := &diskInfo{
		disk: disk,
		node: node,
	}
	c.disks.addDisk(disk.DiskID, diskInfo)
	span.Infof("add disk %v success", disk)
	return nil
}

func (c *cluster) applySetDiskBroken(ctx context.Context, data []byte) error {
	span := trace.SpanFromContextSafe(ctx)
	diskId := c.decodeDiskId(data)

	disk := c.disks.get(diskId)
	if disk == nil {
		span.Errorf("disk %d not found", diskId)
		return apierrors.ErrDiskNotExist
	}

	newDisk := disk.Clone()
	newDisk.disk.Status = proto.DiskStatus_DiskStatusBroken

	err := c.storage.PutDisk(ctx, newDisk.disk)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", disk, err)
		return err
	}

	c.disks.addDisk(newDisk.disk.DiskID, newDisk)
	span.Infof("set disk %v broken success", newDisk.disk)
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

	nodeId := info.Id
	info.State = proto.NodeState_Alive
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

	n := c.allNodes.GetByNameNoLock(info.Addr)
	if n == nil {
		span.Warnf("node[%s] not exist", info.Addr)
		return apierrors.ErrNodeNotExist
	}

	newIfo := n.GetInfo()
	newIfo.Roles = info.Roles

	span.Infof("disk info on node, nodeId %d, disk %v", info.Id, n.dm.disks)

	newNode := &node{
		nodeId:  newIfo.Id,
		info:    newIfo,
		dm:      n.dm,
		expires: time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second),
	}
	
	if err = c.storage.Put(ctx, newIfo); err != nil {
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
	n.HandleHeartbeat(ctx, args.Disks, expires)
	return nil
}
