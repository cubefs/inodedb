package master

import (
	"context"

	"github.com/cubefs/inodedb/proto"
)

type RPCServer struct{}

func (s *RPCServer) Cluster(context.Context, *proto.ClusterRequest) (*proto.ClusterResponse, error) {
	return nil, nil
}

func (s *RPCServer) CreateSpace(context.Context, *proto.CreateSpaceRequest) (*proto.CreateSpaceResponse, error) {
	return nil, nil
}

func (s *RPCServer) DeleteSpace(context.Context, *proto.DeleteSpaceRequest) (*proto.DeleteSpaceResponse, error) {
	return nil, nil
}

func (s *RPCServer) GetSpace(context.Context, *proto.GetSpaceRequest) (*proto.GetSpaceResponse, error) {
	return nil, nil
}

func (s *RPCServer) Heartbeat(context.Context, *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	return nil, nil
}

func (s *RPCServer) Report(context.Context, *proto.ReportRequest) (*proto.ReportResponse, error) {
	return nil, nil
}

func (s *RPCServer) GetNode(context.Context, *proto.GetNodeRequest) (*proto.GetNodeResponse, error) {
	return nil, nil
}
