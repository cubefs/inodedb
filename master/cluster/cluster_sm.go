package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/base"
	"github.com/cubefs/inodedb/proto"
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

func (c *cluster) Apply(ctx context.Context, pds []base.ApplyReq) (rets []base.ApplyRet, err error) {
	rets = make([]base.ApplyRet, 0, len(pds))

	for _, pd1 := range pds {
		pd := pd1.Data
		data := pd.Data
		_, newCtx := trace.StartSpanFromContextWithTraceID(ctx, "", string(pd.Context))
		op := pd.Op
		var ret interface{}
		switch op {
		case RaftOpRegisterNode:
			ret, err = c.applyRegister(newCtx, data)
		case RaftOpUnregisterNode:
			ret, err = c.applyUnregister(newCtx, data)
		case RaftOpHeartbeat:
			ret, err = c.applyHeartbeat(newCtx, data)
		case RaftOpUpdateNode:
			ret, err = c.applyUpdate(newCtx, data)
		case RaftOpAddDisk:
			ret, err = c.applyAddDisk(newCtx, data)
		case RaftOpSetBroken:
			ret, err = c.applySetDiskBroken(newCtx, data)
		default:
			return nil, errors.Info(apierrors.ErrUnknownOperationType, fmt.Sprintf("unknown operation type: %d", op))
		}

		if err != nil {
			return nil, err
		}
		rets = append(rets, base.ApplyRet{Ret: ret, Idx: pd1.Idx})
	}

	return
}

func (c *cluster) Flush(ctx context.Context) error {
	return nil
}

func (c *cluster) LeaderChange(peerID uint64) error {
	return nil
}

func (c *cluster) applyAddDisk(ctx context.Context, data []byte) (ret, err error) {
	span := trace.SpanFromContextSafe(ctx)
	d := &proto.Disk{}
	err = json.Unmarshal(data, d)
	if err != nil {
		span.Errorf("unmarshal disk failed, err: %v", err)
		return nil, err
	}

	ifo := c.disks.get(d.DiskID)
	if ifo != nil {
		span.Errorf("disk %d already exist", d.DiskID)
		return fmt.Errorf("disk %d already exist", d.DiskID), nil
	}

	node := c.allNodes.Get(d.NodeID)
	if node == nil {
		span.Errorf("node %d not found", d.NodeID)
		return apierrors.ErrNodeNotExist, nil
	}

	info := protoDiskToInternalDisk(d)
	info.Status = proto.DiskStatus_DiskStatusNormal
	err = c.storage.PutDisk(ctx, info)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", d, err)
		return nil, err
	}

	disk := newDisk(info, node.GetInfo(), 0)
	c.disks.addDisk(d.DiskID, disk)
	span.Infof("add disk %v success", d)
	return nil, nil
}

func (c *cluster) applySetDiskBroken(ctx context.Context, data []byte) (ret, err error) {
	span := trace.SpanFromContextSafe(ctx)
	diskID := c.decodeDiskID(data)

	disk := c.disks.get(diskID)
	if disk == nil {
		span.Errorf("disk %d not found", diskID)
		return apierrors.ErrDiskNotExist, nil
	}

	info := disk.GetInfo()
	oldStatus := info.Status
	disk.info.Status = proto.DiskStatus_DiskStatusBroken
	err = c.storage.PutDisk(ctx, info)
	if err != nil {
		span.Errorf("put disk to storage failed, disk %v, err: %v", disk, err)
		disk.info.Status = oldStatus
		return nil, err
	}

	span.Infof("set disk %v broken success", disk.DiskID)
	return nil, nil
}

func (c *cluster) applyRegister(ctx context.Context, data []byte) (applyErr, err error) {
	span := trace.SpanFromContextSafe(ctx)

	info := &nodeInfo{}
	err = info.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	if n := c.allNodes.GetByNameNoLock(info.Addr); n != nil {
		span.Warnf("node[%s] has been exist, n %+v", info.Addr, n.GetInfo())
		return apierrors.ErrNodeAlreadyExist, nil
	}

	info.State = proto.NodeState_Alive
	newNode := newNode(info, c.cfg.HeartbeatTimeoutS)
	if err = c.storage.Put(ctx, info); err != nil {
		return nil, err
	}
	c.allNodes.PutNoLock(newNode)
	span.Debugf("register node success, node: %+v", newNode.info)
	return nil, nil
}

func (c *cluster) applyUpdate(ctx context.Context, data []byte) (ret error, err error) {
	span := trace.SpanFromContextSafe(ctx)

	info := &nodeInfo{}
	err = info.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	n := c.allNodes.GetByNameNoLock(info.Addr)
	if n == nil {
		span.Warnf("node[%s] not exist", info.Addr)
		return apierrors.ErrNodeNotExist, nil
	}

	newInfo := n.GetInfo()
	newInfo.Roles = info.Roles

	span.Infof("disk info on node, nodeID %v", info)
	if err = c.storage.Put(ctx, newInfo); err != nil {
		return nil, err
	}

	newNode := newNode(newInfo, c.cfg.HeartbeatTimeoutS)
	c.allNodes.PutNoLock(newNode)
	span.Debugf("update node success, node: %+v", newNode.info)
	return nil, nil
}

func (c *cluster) applyUnregister(ctx context.Context, data []byte) (ret, err error) {
	nodeID := c.decodeNodeID(data)

	c.allNodes.Lock()
	defer c.allNodes.Unlock()

	get := c.allNodes.GetNoLock(nodeID)
	if get == nil {
		return nil, nil
	}

	err = c.storage.Delete(ctx, nodeID)
	if err != nil {
		return nil, err
	}

	c.allNodes.DeleteNoLock(nodeID)
	return nil, nil
}

func (c *cluster) applyHeartbeat(ctx context.Context, data []byte) (error, error) {
	args := &HeartbeatArgs{}
	err := json.Unmarshal(data, args)
	if err != nil {
		return nil, err
	}

	n := c.allNodes.Get(args.NodeID)
	if n == nil {
		return apierrors.ErrNodeNotExist, nil
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	span := trace.SpanFromContext(ctx)
	n.expires = time.Now().Add(time.Duration(c.cfg.HeartbeatTimeoutS) * time.Second)
	if n.info.State != proto.NodeState_Alive {
		n.info.State = proto.NodeState_Alive
	}

	for _, r := range args.Disks {
		d := c.disks.get(r.DiskID)
		if d == nil {
			span.Warnf("disk not found in node disk list, diskID %d, nodeID %d", r.DiskID, n.nodeID)
			continue
		}
		d.updateReport(r)
	}

	return nil, nil
}
