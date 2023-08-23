package cluster

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkVolumes_Put(b *testing.B) {
	n := nodeSet{}
	rand.Seed(time.Now().Unix())
	for i := 0; i < b.N; i++ {
		x := rand.Int()
		nodeInfo := &node{
			nodeId: uint32(x),
			info:   &NodeInfo{Id: uint32(x)},
		}
		n.put(nodeInfo)
	}
}

func BenchmarkVolumes_Delete(b *testing.B) {
	n := nodeSet{}
	rand.Seed(time.Now().Unix())
	for i := 0; i < 3000; i++ {
		x := rand.Int()
		nodeInfo := &node{
			nodeId: uint32(x),
			info:   &NodeInfo{Id: uint32(x)},
		}
		n.put(nodeInfo)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.delete(uint32(i))
	}
}

func TestVolumes_Put(t *testing.T) {
	n := nodeSet{}
	for _, i := range []int{1, 3, 2, 4} {
		nodeInfo := &node{
			nodeId: uint32(i),
			info:   &NodeInfo{Id: uint32(i)},
		}
		n.put(nodeInfo)
	}

	idx, ok := search(n.nodes, 3)
	require.Equal(t, 2, idx)
	require.True(t, ok)

	idx, ok = search(n.nodes, uint32(5))
	require.Equal(t, 4, idx)
	require.False(t, ok)
}

func TestVolumes_Delete(t *testing.T) {
	n := nodeSet{}
	for _, i := range []int{1, 3, 2, 4} {
		nodeInfo := &node{
			nodeId: uint32(i),
			info:   &NodeInfo{Id: uint32(i)},
		}
		n.put(nodeInfo)
	}
	n.delete(uint32(2))

	idx, ok := search(n.nodes, uint32(2))
	require.Equal(t, 1, idx)
	require.False(t, ok)

	idx, ok = search(n.nodes, uint32(3))
	require.Equal(t, 1, idx)
	require.True(t, ok)
}
