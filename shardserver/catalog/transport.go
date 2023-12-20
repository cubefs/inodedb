package catalog

import (
	"context"
	"strconv"
	"sync"

	"google.golang.org/grpc"

	"github.com/cubefs/inodedb/proto"
	"golang.org/x/sync/singleflight"
)

type masterClient interface {
	Cluster(context.Context, *proto.ClusterRequest, ...grpc.CallOption) (*proto.ClusterResponse, error)
	Heartbeat(context.Context, *proto.HeartbeatRequest, ...grpc.CallOption) (*proto.HeartbeatResponse, error)
	Report(context.Context, *proto.ReportRequest, ...grpc.CallOption) (*proto.ReportResponse, error)
	GetNode(context.Context, *proto.GetNodeRequest, ...grpc.CallOption) (*proto.GetNodeResponse, error)
	GetSpace(ctx context.Context, in *proto.GetSpaceRequest, opts ...grpc.CallOption) (*proto.GetSpaceResponse, error)
	GetCatalogChanges(context.Context, *proto.GetCatalogChangesRequest, ...grpc.CallOption) (*proto.GetCatalogChangesResponse, error)
	ListDisks(ctx context.Context, in *proto.ListDiskRequest, opts ...grpc.CallOption) (*proto.ListDiskResponse, error)
	AllocDiskID(ctx context.Context, in *proto.AllocDiskIDRequest, opts ...grpc.CallOption) (*proto.AllocDiskIDResponse, error)
	AddDisk(ctx context.Context, in *proto.AddDiskRequest, opts ...grpc.CallOption) (*proto.AddDiskResponse, error)
}

type transport struct {
	myself       *proto.Node
	allNodes     sync.Map
	done         chan struct{}
	masterClient masterClient
	singleRun    *singleflight.Group
}

func newTransport(masterClient masterClient, myself *proto.Node) *transport {
	return &transport{
		myself:       myself,
		done:         make(chan struct{}),
		masterClient: masterClient,
		singleRun:    &singleflight.Group{},
	}
}

func (t *transport) Register(ctx context.Context) error {
	resp, err := t.masterClient.Cluster(ctx, &proto.ClusterRequest{
		Operation: proto.ClusterOperation_Join,
		NodeInfo: proto.Node{
			Addr:     t.myself.Addr,
			GrpcPort: t.myself.GrpcPort,
			HttpPort: t.myself.HttpPort,
			RaftPort: t.myself.RaftPort,
			Az:       t.myself.Az,
			Roles:    t.myself.Roles,
			State:    proto.NodeState_Alive,
		},
	})
	if err != nil {
		return err
	}

	t.myself.ID = resp.ID
	return nil
}

func (t *transport) GetMyself() *proto.Node {
	node := *t.myself
	return &node
}

func (t *transport) GetSpace(ctx context.Context, sid proto.Sid) (proto.SpaceMeta, error) {
	resp, err := t.masterClient.GetSpace(ctx, &proto.GetSpaceRequest{
		Sid: sid,
	})
	if err != nil {
		return proto.SpaceMeta{}, err
	}

	return resp.Info, nil
}

func (t *transport) GetRouteUpdate(ctx context.Context, routeVersion uint64) (uint64, []proto.CatalogChangeItem, error) {
	resp, err := t.masterClient.GetCatalogChanges(ctx, &proto.GetCatalogChangesRequest{RouteVersion: routeVersion, NodeID: t.myself.ID})
	if err != nil {
		return 0, nil, err
	}

	return resp.RouteVersion, resp.Items, nil
}

func (t *transport) Report(ctx context.Context, infos []proto.ShardReport) ([]proto.ShardTask, error) {
	resp, err := t.masterClient.Report(ctx, &proto.ReportRequest{
		NodeID: t.myself.ID,
		Infos:  infos,
	})
	if err != nil {
		return nil, err
	}

	return resp.Tasks, err
}

func (t *transport) GetNode(ctx context.Context, nodeID uint32) (*proto.Node, error) {
	v, ok := t.allNodes.Load(nodeID)
	if ok {
		return v.(*proto.Node), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(nodeID)), func() (interface{}, error) {
		resp, err := t.masterClient.GetNode(ctx, &proto.GetNodeRequest{ID: nodeID})
		if err != nil {
			return nil, err
		}
		newNode := &proto.Node{
			ID:       nodeID,
			Addr:     resp.NodeInfo.Addr,
			GrpcPort: resp.NodeInfo.GrpcPort,
			HttpPort: resp.NodeInfo.HttpPort,
			RaftPort: resp.NodeInfo.RaftPort,
			Az:       resp.NodeInfo.Az,
			Roles:    resp.NodeInfo.Roles,
			State:    resp.NodeInfo.State,
		}
		t.allNodes.Store(nodeID, newNode)
		return newNode, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*proto.Node), err
}

func (t *transport) ListDisks(ctx context.Context) ([]proto.Disk, error) {
	resp, err := t.masterClient.ListDisks(ctx, &proto.ListDiskRequest{
		NodeID: t.myself.ID,
		Count:  10000,
	})
	if err != nil {
		return nil, err
	}

	return resp.Disks, nil
}

func (t *transport) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	resp, err := t.masterClient.AllocDiskID(ctx, &proto.AllocDiskIDRequest{})
	if err != nil {
		return 0, err
	}

	return resp.DiskID, nil
}

func (t *transport) RegisterDisk(ctx context.Context, disk proto.Disk) error {
	_, err := t.masterClient.AddDisk(ctx, &proto.AddDiskRequest{
		Disk: disk,
	})
	return err
}

func (t *transport) HeartbeatDisks(ctx context.Context, disks []proto.DiskReport) error {
	_, err := t.masterClient.Heartbeat(ctx, &proto.HeartbeatRequest{NodeID: t.myself.ID, Disks: disks})
	return err
}

func (t *transport) Close() {
	close(t.done)
}
