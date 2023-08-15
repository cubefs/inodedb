package catalog

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/proto"
	"golang.org/x/sync/singleflight"
)

type masterClient interface {
	Cluster(context.Context, *proto.ClusterRequest) (*proto.ClusterResponse, error)
	Heartbeat(context.Context, *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error)
	Report(context.Context, *proto.ReportRequest) (*proto.ReportResponse, error)
	GetNode(context.Context, *proto.GetNodeRequest) (*proto.GetNodeResponse, error)
	GetCatalogChanges(context.Context, *proto.GetCatalogChangesRequest) (*proto.GetCatalogChangesResponse, error)
}

type nodeInfo struct {
	id          uint32
	addr        string
	grpcPort    uint32
	httpPort    uint32
	replicaPort uint32
	az          string
	role        proto.NodeRole
	state       proto.NodeState
}

type transport struct {
	myself       nodeInfo
	allNodes     sync.Map
	done         chan struct{}
	masterClient masterClient
	singleRun    *singleflight.Group
}

func (t *transport) Register(ctx context.Context) error {
	resp, err := t.masterClient.Cluster(ctx, &proto.ClusterRequest{
		Operation: proto.ClusterOperation_Join,
		NodeInfo: &proto.Node{
			Addr:        t.myself.addr,
			GrpcPort:    t.myself.grpcPort,
			HttpPort:    t.myself.httpPort,
			ReplicaPort: t.myself.replicaPort,
			Az:          t.myself.az,
			Role:        proto.NodeRole_ShardServer,
			State:       proto.NodeState_Alive,
		},
	})
	if err != nil {
		return err
	}
	t.myself.id = resp.Id
	return nil
}

func (t *transport) GetRouteUpdate(ctx context.Context, routeVersion uint64) ([]*proto.RouteItem, error) {
	resp, err := t.masterClient.GetCatalogChanges(ctx, &proto.GetCatalogChangesRequest{RouteVersion: routeVersion, NodeId: t.myself.id})
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func (t *transport) Report(ctx context.Context, infos []*proto.ShardReport) error {
	_, err := t.masterClient.Report(ctx, &proto.ReportRequest{
		Id:    t.myself.id,
		Infos: infos,
	})
	return err
}

func (t *transport) GetNode(ctx context.Context, nodeId uint32) (*nodeInfo, error) {
	v, ok := t.allNodes.Load(nodeId)
	if ok {
		return v.(*nodeInfo), nil
	}
	v, err, _ := t.singleRun.Do(strconv.Itoa(int(nodeId)), func() (interface{}, error) {
		resp, err := t.masterClient.GetNode(ctx, &proto.GetNodeRequest{Id: nodeId})
		if err != nil {
			return nil, err
		}
		newNode := &nodeInfo{
			id:          nodeId,
			addr:        resp.NodeInfo.Addr,
			grpcPort:    resp.NodeInfo.GrpcPort,
			httpPort:    resp.NodeInfo.HttpPort,
			replicaPort: resp.NodeInfo.ReplicaPort,
			az:          resp.NodeInfo.Az,
			role:        resp.NodeInfo.Role,
			state:       resp.NodeInfo.State,
		}
		t.allNodes.Store(nodeId, newNode)
		return newNode, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*nodeInfo), err
}

func (t *transport) StartHeartbeat(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	defer heartbeatTicker.Stop()
	span := trace.SpanFromContext(ctx)

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				if _, err := t.masterClient.Heartbeat(ctx, &proto.HeartbeatRequest{Id: t.myself.id}); err != nil {
					span.Warnf("heartbeat to master failed: %s", err)
				}
			case <-t.done:
				return
			}
		}
	}()
}

func (t *transport) Close() {
	close(t.done)
}
