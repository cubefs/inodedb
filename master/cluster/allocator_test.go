package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocMgr_Alloc(t *testing.T) {
	ctx := context.Background()
	mgr := NewShardServerAllocator(ctx)
	az := "test_az"
	rack := "test_rack"
	for i := 0; i < 5; i++ {
		n := &node{
			nodeId: uint32(i),
			info: &nodeInfo{
				Id:   uint32(i),
				Addr: "test_addr" + string(rune(i)),
				Az:   az,
				Rack: rack,
			},
		}
		mgr.Put(ctx, n)
	}

	alloc, err := mgr.Alloc(ctx, &AllocArgs{
		Count:    1,
		AZ:       az,
		RackWare: false,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(alloc))

	alloc, err = mgr.Alloc(ctx, &AllocArgs{
		Count:    2,
		AZ:       az,
		RackWare: true,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(alloc))
}
