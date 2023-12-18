package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/cubefs/inodedb/proto"
	"github.com/stretchr/testify/require"
)

func TestAllocMgr_Alloc(t *testing.T) {
	ctx := context.Background()
	mgr := NewShardServerAllocator(ctx)
	az := "test_az"
	rack := "test_rack"
	numNodes := 5
	for i := 0; i < numNodes; i++ {
		n := &node{
			nodeId: uint32(i),
			info: &nodeInfo{
				Id:    uint32(i),
				Addr:  "test_addr" + string(rune(i)),
				Az:    az,
				Rack:  rack,
				State: proto.NodeState_Alive,
			},
			expires: time.Now().Add(30 * time.Second),
		}
		ifo := &DiskInfo{
			disk: &proto.Disk{
				DiskID: uint32(i),
				Status: proto.DiskStatus_DiskStatusNormal,
			},
			node: n,
		}
		mgr.Put(ctx, ifo)
	}

	alloc, err := mgr.Alloc(ctx, &AllocArgs{
		Count:    2,
		AZ:       az,
		RackWare: false,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(alloc))

	alloc, err = mgr.Alloc(ctx, &AllocArgs{
		Count:    2,
		AZ:       az,
		RackWare: true,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(alloc))

	alloc, err = mgr.Alloc(ctx, &AllocArgs{
		Count:    numNodes + 1,
		AZ:       az,
		RackWare: false,
	})
	require.Error(t, err)
	require.Equal(t, numNodes, len(alloc))
}
