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
	ShardDiskID() proto.DiskID
	ShardInsertItem(ctx context.Context, in *proto.ShardInsertItemRequest, opts ...grpc.CallOption) (*proto.ShardInsertItemResponse, error)
	ShardUpdateItem(ctx context.Context, in *proto.ShardUpdateItemRequest, opts ...grpc.CallOption) (*proto.ShardUpdateItemResponse, error)
	ShardDeleteItem(ctx context.Context, in *proto.ShardDeleteItemRequest, opts ...grpc.CallOption) (*proto.ShardDeleteItemResponse, error)
	ShardGetItem(ctx context.Context, in *proto.ShardGetItemRequest, opts ...grpc.CallOption) (*proto.ShardGetItemResponse, error)
	ShardLink(ctx context.Context, in *proto.ShardLinkRequest, opts ...grpc.CallOption) (*proto.ShardLinkResponse, error)
	ShardUnlink(ctx context.Context, in *proto.ShardUnlinkRequest, opts ...grpc.CallOption) (*proto.ShardUnlinkResponse, error)
	ShardList(ctx context.Context, in *proto.ShardListRequest, opts ...grpc.CallOption) (*proto.ShardListResponse, error)
	ShardSearch(ctx context.Context, in *proto.ShardSearchRequest, opts ...grpc.CallOption) (*proto.ShardSearchResponse, error)
}

type ShardServerConfig struct {
	TransportConfig sc.TransportConfig `json:"transport"`
	MasterClient    *sc.MasterClient   `json:"-"`
}

type transport struct {
	info              *proto.Node
	masterClient      *sc.MasterClient
	shardServerClient *sc.ShardServerClient

	done chan struct{}
	lock sync.RWMutex
}

func NewTransport(cfg *ShardServerConfig, info *proto.Node) (*transport, error) {
	serverClient, err := sc.NewShardServerClient(&sc.ShardServerConfig{
		MasterClient:    cfg.MasterClient,
		TransportConfig: cfg.TransportConfig,
	})
	if err != nil {
		return nil, err
	}
	return &transport{
		shardServerClient: serverClient,
		info:              info,
		done:              make(chan struct{}),
		masterClient:      cfg.MasterClient,
	}, nil
}

func (s *transport) Register(ctx context.Context) error {
	resp, err := s.masterClient.Cluster(ctx, &proto.ClusterRequest{
		Operation: proto.ClusterOperation_Join,
		NodeInfo: proto.Node{
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
	s.info.ID = resp.ID
	return nil
}

func (s *transport) StartHeartbeat(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	span := trace.SpanFromContext(ctx)

	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				if _, err := s.masterClient.Heartbeat(ctx, &proto.HeartbeatRequest{NodeID: s.info.ID}); err != nil {
					span.Warnf("heartbeat to master failed: %s", err)
				}
			case <-s.done:
				return
			}
		}
	}()
}

func (s *transport) GetClient(ctx context.Context, diskID proto.DiskID) (ShardServerClient, error) {
	client, err := s.shardServerClient.GetShardServerClient(ctx, diskID)
	if err != nil {
		return nil, err
	}
	return &shardClient{diskID: diskID, InodeDBShardServerClient: client}, nil
}

func (s *transport) Close() {
	close(s.done)
	if s.shardServerClient != nil {
		s.shardServerClient.Close()
	}
}

type shardClient struct {
	diskID proto.DiskID
	proto.InodeDBShardServerClient
}

func (s *shardClient) ShardDiskID() proto.DiskID {
	return s.diskID
}
