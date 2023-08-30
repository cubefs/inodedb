package catalog

import (
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	sc "github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type service struct {
	info         *proto.Node
	masterClient *sc.MasterClient

	done chan struct{}
	lock sync.RWMutex
}

func NewService(masterClient *sc.MasterClient, info *proto.Node) *service {
	return &service{
		info:         info,
		done:         make(chan struct{}),
		masterClient: masterClient,
	}
}

func (s *service) Register(ctx context.Context) error {
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

func (s *service) StartHeartbeat(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	defer heartbeatTicker.Stop()
	span := trace.SpanFromContext(ctx)

	go func() {
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

func (s *service) Close() {
	close(s.done)
}
