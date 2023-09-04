package catalog

import (
	"context"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/cubefs/cubefs/blobstore/common/trace"
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
}

type transporter struct {
	myself       *proto.Node
	allNodes     sync.Map
	done         chan struct{}
	masterClient masterClient
	singleRun    *singleflight.Group
}

func newTransporter(masterClient masterClient, myself *proto.Node) *transporter {
	return &transporter{
		myself:       myself,
		done:         make(chan struct{}),
		masterClient: masterClient,
		singleRun:    &singleflight.Group{},
	}
}

func (t *transporter) Register(ctx context.Context) error {
	resp, err := t.masterClient.Cluster(ctx, &proto.ClusterRequest{
		Operation: proto.ClusterOperation_Join,
		NodeInfo: &proto.Node{
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

	t.myself.Id = resp.Id
	return nil
}

func (t *transporter) GetMyself() *proto.Node {
	node := *t.myself
	return &node
}

func (t *transporter) GetSpace(ctx context.Context, spaceName string) (*proto.SpaceMeta, error) {
	resp, err := t.masterClient.GetSpace(ctx, &proto.GetSpaceRequest{
		Name: spaceName,
	})
	if err != nil {
		return nil, err
	}

	return resp.Info, nil
}

func (t *transporter) GetRouteUpdate(ctx context.Context, routeVersion uint64) (uint64, []*proto.CatalogChangeItem, error) {
	resp, err := t.masterClient.GetCatalogChanges(ctx, &proto.GetCatalogChangesRequest{RouteVersion: routeVersion, NodeId: t.myself.Id})
	if err != nil {
		return 0, nil, err
	}

	return resp.RouteVersion, resp.Items, nil
}

func (t *transporter) Report(ctx context.Context, infos []*proto.ShardReport) ([]*proto.ShardTask, error) {
	resp, err := t.masterClient.Report(ctx, &proto.ReportRequest{
		Id:    t.myself.Id,
		Infos: infos,
	})
	if err != nil {
		return nil, err
	}

	return resp.Tasks, err
}

func (t *transporter) GetNode(ctx context.Context, nodeId uint32) (*proto.Node, error) {
	v, ok := t.allNodes.Load(nodeId)
	if ok {
		return v.(*proto.Node), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(nodeId)), func() (interface{}, error) {
		resp, err := t.masterClient.GetNode(ctx, &proto.GetNodeRequest{Id: nodeId})
		if err != nil {
			return nil, err
		}
		newNode := &proto.Node{
			Id:       nodeId,
			Addr:     resp.NodeInfo.Addr,
			GrpcPort: resp.NodeInfo.GrpcPort,
			HttpPort: resp.NodeInfo.HttpPort,
			RaftPort: resp.NodeInfo.RaftPort,
			Az:       resp.NodeInfo.Az,
			Roles:    resp.NodeInfo.Roles,
			State:    resp.NodeInfo.State,
		}
		t.allNodes.Store(nodeId, newNode)
		return newNode, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*proto.Node), err
}

func (t *transporter) StartHeartbeat(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	defer heartbeatTicker.Stop()
	span := trace.SpanFromContext(ctx)

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				if _, err := t.masterClient.Heartbeat(ctx, &proto.HeartbeatRequest{Id: t.myself.Id}); err != nil {
					span.Warnf("heartbeat to master failed: %s", err)
				}
			case <-t.done:
				return
			}
		}
	}()
}

func (t *transporter) Close() {
	close(t.done)
}
