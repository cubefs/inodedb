package catalog

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type ShardServerClient interface {
	ShardInsertItem(ctx context.Context, in *proto.InsertItemRequest, opts ...grpc.CallOption) (*proto.InsertItemResponse, error)
	ShardUpdateItem(ctx context.Context, in *proto.UpdateItemRequest, opts ...grpc.CallOption) (*proto.UpdateItemResponse, error)
	ShardDeleteItem(ctx context.Context, in *proto.DeleteItemRequest, opts ...grpc.CallOption) (*proto.DeleteItemResponse, error)
	ShardGetItem(ctx context.Context, in *proto.GetItemRequest, opts ...grpc.CallOption) (*proto.GetItemResponse, error)
	ShardLink(ctx context.Context, in *proto.LinkRequest, opts ...grpc.CallOption) (*proto.LinkResponse, error)
	ShardUnlink(ctx context.Context, in *proto.UnlinkRequest, opts ...grpc.CallOption) (*proto.UnlinkResponse, error)
	ShardList(ctx context.Context, in *proto.ListRequest, opts ...grpc.CallOption) (*proto.ListResponse, error)
	ShardSearch(ctx context.Context, in *proto.SearchRequest, opts ...grpc.CallOption) (*proto.SearchResponse, error)
}

type ShardServerConfig struct {
	TransportConfig sc.TransportConfig `json:"transport"`
	MasterClient    *sc.MasterClient   `json:"-"`
}

type transporter struct {
	info              *proto.Node
	masterClient      *sc.MasterClient
	shardServerClient *sc.ShardServerClient

	done chan struct{}
	lock sync.RWMutex
}

var defaultTransporter *transporter

func NewTransporter(cfg *ShardServerConfig, info *proto.Node) (*transporter, error) {
	serverClient, err := sc.NewShardServerClient(&sc.ShardServerConfig{
		MasterClient:    cfg.MasterClient,
		TransportConfig: cfg.TransportConfig,
	})
	if err != nil {
		return nil, err
	}
	defaultTransporter = &transporter{
		shardServerClient: serverClient,
		info:              info,
		done:              make(chan struct{}),
		masterClient:      cfg.MasterClient,
	}
	return defaultTransporter, nil
}

func GetDefaultTransporter() *transporter {
	return defaultTransporter
}

func (s *transporter) Register(ctx context.Context) error {
	resp, err := s.masterClient.Cluster(ctx, &proto.ClusterRequest{
		Operation: proto.ClusterOperation_Join,
		NodeInfo: &proto.Node{
			Addr:     s.info.Addr,
			GrpcPort: s.info.GrpcPort,
			HttpPort: s.info.HttpPort,
			RaftPort: s.info.RaftPort,
			Az:       s.info.Az,
			Rack:     s.info.Rack,
			Roles:    s.info.Roles,
			State:    proto.NodeState_Alive,
		},
	})
	if err != nil {
		return err
	}
	s.info.Id = resp.Id
	return nil
}

func (s *transporter) StartHeartbeat(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	span := trace.SpanFromContext(ctx)

	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				if _, err := s.masterClient.Heartbeat(ctx, &proto.HeartbeatRequest{Id: s.info.Id}); err != nil {
					span.Warnf("heartbeat to master failed: %s", err)
				}
			case <-s.done:
				return
			}
		}
	}()
}

func (s *transporter) GetClient(ctx context.Context, nodeId uint32) (ShardServerClient, error) {
	client, err := s.shardServerClient.GetClient(ctx, nodeId)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (s *transporter) Close() {
	close(s.done)
	if s.shardServerClient != nil {
		s.shardServerClient.Close()
	}
}
