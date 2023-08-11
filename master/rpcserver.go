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
