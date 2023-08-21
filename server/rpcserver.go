// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var auditLogPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type RPCServer struct {
	*Server
}

func NewRPCServer(server *Server) *RPCServer {
	rs := &RPCServer{Server: server}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(rs.unaryInterceptorWithTracer, rs.unaryInterceptorWithTracer))
	if rs.master != nil {
		proto.RegisterInodeDBMasterServer(s, rs)
	}
	if rs.router != nil {
		proto.RegisterInodeDBRouterServer(s, rs)
	}
	if rs.shardServer != nil {
		proto.RegisterInodeDBShardServerServer(s, rs)
	}
	return nil
}

// Master API

func (r *RPCServer) Cluster(context.Context, *proto.ClusterRequest) (*proto.ClusterResponse, error) {
	return nil, nil
}

func (r *RPCServer) CreateSpace(context.Context, *proto.CreateSpaceRequest) (*proto.CreateSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) DeleteSpace(context.Context, *proto.DeleteSpaceRequest) (*proto.DeleteSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetSpace(context.Context, *proto.GetSpaceRequest) (*proto.GetSpaceResponse, error) {
	return nil, nil
}

func (r *RPCServer) Heartbeat(context.Context, *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	return nil, nil
}

func (r *RPCServer) Report(context.Context, *proto.ReportRequest) (*proto.ReportResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetNode(context.Context, *proto.GetNodeRequest) (*proto.GetNodeResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetCatalogChanges(context.Context, *proto.GetCatalogChangesRequest) (*proto.GetCatalogChangesResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetRoleNodes(context.Context, *proto.GetRoleNodesRequest) (*proto.GetRoleNodesResponse, error) {
	return nil, nil
}

// Shard Server API

func (r *RPCServer) AddShard(ctx context.Context, req *proto.AddShardRequest) (*proto.AddShardResponse, error) {
	shardServer := r.shardServer
	err := shardServer.Catalog.AddShard(ctx, req.SpaceName, req.ShardId, req.RouteVersion, req.InoLimit, req.Replicates)
	return nil, err
}

func (r *RPCServer) GetShard(ctx context.Context, req *proto.GetShardRequest) (*proto.GetShardResponse, error) {
	shardServer := r.shardServer
	shard, err := shardServer.Catalog.GetShard(ctx, req.SpaceName, req.ShardId)
	if err != nil {
		return nil, err
	}

	return &proto.GetShardResponse{Shard: shard}, nil
}

func (r *RPCServer) ShardInsertItem(ctx context.Context, req *proto.InsertItemRequest) (*proto.InsertItemResponse, error) {
	if req.PreferredShard == 0 {
		return nil, errors.ErrInvalidShardID
	}
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	ino, err := space.InsertItem(ctx, req.PreferredShard, req.Item)
	if err != nil {
		return nil, err
	}

	return &proto.InsertItemResponse{Ino: ino}, nil
}

func (r *RPCServer) ShardUpdateItem(ctx context.Context, req *proto.UpdateItemRequest) (*proto.UpdateItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.UpdateItem(ctx, req.Item)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) ShardDeleteItem(ctx context.Context, req *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.DeleteItem(ctx, req.Ino)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) ShardGetItem(ctx context.Context, req *proto.GetItemRequest) (*proto.GetItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	item, err := space.GetItem(ctx, req.Ino)
	if err != nil {
		return nil, err
	}
	return &proto.GetItemResponse{Item: item}, nil
}

func (r *RPCServer) ShardLink(ctx context.Context, req *proto.LinkRequest) (*proto.LinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.Link(ctx, req.Link)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) ShardUnlink(ctx context.Context, req *proto.UnlinkRequest) (*proto.UnlinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	err = space.Unlink(ctx, req.Unlink)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *RPCServer) ShardList(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	if req.Num > maxListNum {
		return nil, errors.ErrListNumExceed
	}
	shardServer := r.shardServer
	space, err := shardServer.Catalog.GetSpace(ctx, req.SpaceName)
	if err != nil {
		return nil, err
	}

	links, err := space.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return &proto.ListResponse{Links: links}, nil
}

func (r *RPCServer) ShardSearch(context.Context, *proto.SearchRequest) (*proto.SearchResponse, error) {
	return nil, nil
}

// Router API

func (r *RPCServer) InsertItem(context.Context, *proto.InsertItemRequest) (*proto.InsertItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) UpdateItem(context.Context, *proto.UpdateItemRequest) (*proto.UpdateItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) DeleteItem(context.Context, *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) GetItem(context.Context, *proto.GetItemRequest) (*proto.GetItemResponse, error) {
	return nil, nil
}

func (r *RPCServer) Link(context.Context, *proto.LinkRequest) (*proto.LinkResponse, error) {
	return nil, nil
}

func (r *RPCServer) Unlink(context.Context, *proto.UnlinkRequest) (*proto.UnlinkResponse, error) {
	return nil, nil
}

func (r *RPCServer) List(context.Context, *proto.ListRequest) (*proto.ListResponse, error) {
	return nil, nil
}

func (r *RPCServer) Search(context.Context, *proto.SearchRequest) (*proto.SearchResponse, error) {
	return nil, nil
}

// util function

func (r *RPCServer) unaryInterceptorWithTracer(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "failed to get metadata")
	}
	reqId, ok := md[proto.ReqIdKey]
	if ok {
		trace.StartSpanFromContextWithTraceID(ctx, "", reqId[0])
	} else {
		trace.SpanFromContextSafe(ctx)
	}

	resp, err = handler(ctx, req)
	return
}

func (r *RPCServer) unaryInterceptorWithAuditLog(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// TODO: decode into node role
	nodeRole := proto.NodeRole_Router
	start := time.Now()

	resp, err = handler(ctx, req)

	// TODO: record audit log only in specified method

	if info.FullMethod == "" {
		// TODO: fulfill audit log format and content, optimized time cost
		in, _ := json.Marshal(req)
		out, _ := json.Marshal(resp)
		duration := int64(time.Since(start) / time.Millisecond)
		bw := auditLogPool.Get().(*bytes.Buffer)
		defer auditLogPool.Put(bw)
		bw.Reset()
		bw.Write([]byte(info.FullMethod))
		bw.Write([]byte("\t"))
		bw.Write(in)
		bw.Write([]byte("\t"))
		bw.Write(out)
		bw.Write([]byte("\t"))
		bw.Write([]byte(strconv.FormatInt(duration, 10)))
		r.auditLogRecorder[nodeRole].Log([]byte("XX"))
	}

	return
}
