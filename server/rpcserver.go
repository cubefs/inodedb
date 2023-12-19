package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

var auditLogPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type RPCServer struct {
	grpcServer *grpc.Server

	*Server
}

func NewRPCServer(server *Server) *RPCServer {
	rs := &RPCServer{Server: server}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(rs.unaryInterceptorWithTracer, rs.unaryInterceptorWithAuditLog))
	if rs.master != nil {
		proto.RegisterInodeDBMasterServer(s, rs)
	}
	if rs.router != nil {
		proto.RegisterInodeDBRouterServer(s, rs)
	}
	if rs.shardServer != nil {
		proto.RegisterInodeDBShardServerServer(s, rs)
	}

	rs.grpcServer = s
	return rs
}

func (r *RPCServer) Serve(addr string) {
	Listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen addr[%s] failed: %s", addr, err)
	}

	go r.grpcServer.Serve(Listener)

	if r.cfg.Roles[0] == proto.NodeRole_Single.String() {
		time.Sleep(100 * time.Millisecond)
		r.initModuleHandler[proto.NodeRole_ShardServer]()
		time.Sleep(100 * time.Millisecond)
		r.initModuleHandler[proto.NodeRole_Router]()
	}
}

func (r *RPCServer) Stop() {
	r.grpcServer.GracefulStop()
}

// Master API

func (r *RPCServer) Cluster(ctx context.Context, req *proto.ClusterRequest) (*proto.ClusterResponse, error) {
	nodeID, err := r.master.Register(ctx, &req.NodeInfo)
	return &proto.ClusterResponse{ID: nodeID}, err
}

func (r *RPCServer) CreateSpace(ctx context.Context, req *proto.CreateSpaceRequest) (*proto.CreateSpaceResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.Infof("receive CreateSpace request: %+v", req)
	err := r.master.CreateSpace(ctx, req.Name, req.Type, req.DesiredShards, req.FixedFields)
	if err != nil {
		span.Errorf("create space failed: %s", errors.Detail(err))
		return nil, err
	}

	spaceMeta, err := r.master.GetSpaceByName(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	ret := &proto.CreateSpaceResponse{Info: *spaceMeta}
	return ret, nil
}

func (r *RPCServer) DeleteSpace(ctx context.Context, req *proto.DeleteSpaceRequest) (*proto.DeleteSpaceResponse, error) {
	span := trace.SpanFromContext(ctx)
	err := r.master.DeleteSpaceByName(ctx, req.Name)
	if err != nil {
		span.Errorf("delete space[%s] failed: %s", req.Name, errors.Detail(err))
	}
	return nil, err
}

func (r *RPCServer) GetSpace(ctx context.Context, req *proto.GetSpaceRequest) (*proto.GetSpaceResponse, error) {
	var spaceMeta *proto.SpaceMeta
	var err error
	if req.Name != "" {
		spaceMeta, err = r.master.GetSpaceByName(ctx, req.Name)
	} else {
		spaceMeta, err = r.master.GetSpace(ctx, req.Sid)
	}
	
	if err != nil {
		return nil, err
	}

	ret := &proto.GetSpaceResponse{Info: *spaceMeta}
	return ret, nil
}

func (r *RPCServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	span := trace.SpanFromContext(ctx)
	err := r.master.HandleHeartbeat(ctx, &cluster.HeartbeatArgs{
		NodeID: req.NodeID,
		Disks:  req.Disks,
	})
	if err != nil {
		span.Errorf("handle heartbeat failed: %s", err)
	}
	return &proto.HeartbeatResponse{}, err
}

func (r *RPCServer) Report(ctx context.Context, req *proto.ReportRequest) (*proto.ReportResponse, error) {
	span := trace.SpanFromContext(ctx)
	tasks, err := r.master.Report(ctx, req.NodeID, req.Infos)
	if err != nil {
		span.Errorf("report failed: %s", errors.Detail(err))
		return nil, err
	}

	return &proto.ReportResponse{Tasks: tasks}, nil
}

func (r *RPCServer) GetNode(ctx context.Context, req *proto.GetNodeRequest) (*proto.GetNodeResponse, error) {
	node, err := r.master.GetNode(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	return &proto.GetNodeResponse{NodeInfo: *node}, nil
}

func (r *RPCServer) GetCatalogChanges(ctx context.Context, req *proto.GetCatalogChangesRequest) (*proto.GetCatalogChangesResponse, error) {
	span := trace.SpanFromContext(ctx)
	routeVersion, items, err := r.master.GetCatalogChanges(ctx, req.RouteVersion, req.NodeID)
	if err != nil {
		span.Errorf("get catalog changes failed: %s", err)
		return nil, err
	}

	return &proto.GetCatalogChangesResponse{
		RouteVersion: routeVersion,
		Items:        items,
	}, nil
}

func (r *RPCServer) GetRoleNodes(ctx context.Context, req *proto.GetRoleNodesRequest) (*proto.GetRoleNodesResponse, error) {
	nodes, err := r.master.ListNodeInfo(ctx, []proto.NodeRole{req.Role})
	if err != nil {
		return nil, err
	}

	return &proto.GetRoleNodesResponse{Nodes: nodes}, nil
}

func (r *RPCServer) AllocDiskID(ctx context.Context, req *proto.AllocDiskIDRequest) (*proto.AllocDiskIDResponse, error) {
	id, err := r.master.AllocDiskID(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.AllocDiskIDResponse{DiskID: id}, nil
}

func (r *RPCServer) AddDisk(ctx context.Context, req *proto.AddDiskRequest) (*proto.AddDiskResponse, error) {
	disk := req.Disk
	err := r.master.AddDisk(ctx, &disk)
	if err != nil {
		return nil, err
	}
	return &proto.AddDiskResponse{}, nil
}

func (r *RPCServer) ListDisks(ctx context.Context, req *proto.ListDiskRequest) (*proto.ListDiskResponse, error) {
	disks, marker, err := r.master.ListDisk(ctx, req)
	if err != nil {
		return nil, err
	}
	resp := &proto.ListDiskResponse{
		Disks:  disks,
		Marker: marker,
	}
	return resp, nil
}

func (r *RPCServer) GetDisk(ctx context.Context, req *proto.GetDiskRequest) (*proto.GetDiskResponse, error) {
	disk, err := r.master.GetDisk(ctx, req.DiskID)
	if err != nil {
		return nil, err
	}
	return &proto.GetDiskResponse{Disk: disk}, nil
}

func (r *RPCServer) DiskSetBroken(ctx context.Context, req *proto.SetBrokenRequest) (*proto.SetBrokenResponse, error) {
	err := r.master.SetBroken(ctx, req.DiskID)
	if err != nil {
		return nil, err
	}
	return &proto.SetBrokenResponse{}, nil
}

// Shard Server API

func (r *RPCServer) AddShard(ctx context.Context, req *proto.AddShardRequest) (*proto.AddShardResponse, error) {
	shardServer := r.shardServer
	err := shardServer.AddShard(ctx, req.DiskID, req.Sid, req.ShardID, req.Epoch, req.InoLimit, req.Nodes)
	return &proto.AddShardResponse{}, err
}

func (r *RPCServer) UpdateShard(ctx context.Context, req *proto.UpdateShardRequest) (*proto.UpdateShardResponse, error) {
	shardServer := r.shardServer
	err := shardServer.UpdateShard(ctx, req.DiskID, req.Sid, req.ShardID, 0)
	return &proto.UpdateShardResponse{}, err
}

func (r *RPCServer) GetShard(ctx context.Context, req *proto.GetShardRequest) (*proto.GetShardResponse, error) {
	shardServer := r.shardServer
	shard, err := shardServer.GetShardInfo(ctx, req.DiskID, req.Sid, req.ShardID)
	if err != nil {
		return nil, err
	}

	return &proto.GetShardResponse{Shard: shard}, nil
}

func (r *RPCServer) ShardInsertItem(ctx context.Context, req *proto.ShardInsertItemRequest) (*proto.ShardInsertItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	ino, err := space.InsertItem(ctx, req.Header, req.Item)
	if err != nil {
		return nil, err
	}
	return &proto.ShardInsertItemResponse{Ino: ino}, nil
}

func (r *RPCServer) ShardUpdateItem(ctx context.Context, req *proto.ShardUpdateItemRequest) (*proto.ShardUpdateItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	err = space.UpdateItem(ctx, req.Header, req.Item)
	return &proto.ShardUpdateItemResponse{}, err
}

func (r *RPCServer) ShardDeleteItem(ctx context.Context, req *proto.ShardDeleteItemRequest) (*proto.ShardDeleteItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	err = space.DeleteItem(ctx, req.Header, req.Ino)
	return &proto.ShardDeleteItemResponse{}, err
}

func (r *RPCServer) ShardGetItem(ctx context.Context, req *proto.ShardGetItemRequest) (*proto.ShardGetItemResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	item, err := space.GetItem(ctx, req.Header, req.Ino)
	if err != nil {
		return nil, err
	}
	return &proto.ShardGetItemResponse{Item: item}, nil
}

func (r *RPCServer) ShardLink(ctx context.Context, req *proto.ShardLinkRequest) (*proto.ShardLinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	err = space.Link(ctx, req.Header, req.Link)
	if err != nil {
		return nil, err
	}
	return &proto.ShardLinkResponse{}, nil
}

func (r *RPCServer) ShardUnlink(ctx context.Context, req *proto.ShardUnlinkRequest) (*proto.ShardUnlinkResponse, error) {
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	err = space.Unlink(ctx, req.Header, req.Unlink)
	if err != nil {
		return nil, err
	}
	return &proto.ShardUnlinkResponse{}, nil
}

func (r *RPCServer) ShardList(ctx context.Context, req *proto.ShardListRequest) (*proto.ShardListResponse, error) {
	if req.Num > maxListNum {
		return nil, apierrors.ErrListNumExceed
	}
	shardServer := r.shardServer
	space, err := shardServer.GetSpace(ctx, req.Header.Sid)
	if err != nil {
		return nil, err
	}

	links, err := space.List(ctx, req.Header, req.Ino, req.Start, req.Num)
	if err != nil {
		return nil, err
	}
	return &proto.ShardListResponse{Links: links}, nil
}

func (r *RPCServer) ShardSearch(context.Context, *proto.ShardSearchRequest) (*proto.ShardSearchResponse, error) {
	return nil, nil
}

// Router API

func (r *RPCServer) InsertItem(ctx context.Context, req *proto.InsertItemRequest) (*proto.InsertItemResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	ino, err := space.InsertItem(ctx, req.Item)
	if err != nil {
		return nil, err
	}
	return &proto.InsertItemResponse{Ino: ino}, nil
}

func (r *RPCServer) UpdateItem(ctx context.Context, req *proto.UpdateItemRequest) (*proto.UpdateItemResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	return &proto.UpdateItemResponse{}, space.UpdateItem(ctx, req.Item)
}

func (r *RPCServer) DeleteItem(ctx context.Context, req *proto.DeleteItemRequest) (*proto.DeleteItemResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteItemResponse{}, space.DeleteItem(ctx, req.Ino)
}

func (r *RPCServer) GetItem(ctx context.Context, req *proto.GetItemRequest) (*proto.GetItemResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	item, err := space.GetItem(ctx, req.Ino)
	if err != nil {
		return nil, err
	}
	return &proto.GetItemResponse{Item: item}, nil
}

func (r *RPCServer) Link(ctx context.Context, req *proto.LinkRequest) (*proto.LinkResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	return &proto.LinkResponse{}, space.Link(ctx, req.Link)
}

func (r *RPCServer) Unlink(ctx context.Context, req *proto.UnlinkRequest) (*proto.UnlinkResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	return &proto.UnlinkResponse{}, space.Unlink(ctx, req.Unlink)
}

func (r *RPCServer) List(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	links, err := space.List(ctx, req)
	if err != nil {
		return nil, err
	}
	return &proto.ListResponse{Links: links}, nil
}

func (r *RPCServer) Search(ctx context.Context, req *proto.SearchRequest) (*proto.SearchResponse, error) {
	spaceName := req.SpaceName
	space, err := r.router.GetSpace(ctx, spaceName)
	if err != nil {
		return nil, err
	}
	return space.Search(ctx, req)
}

// util function

func (r *RPCServer) unaryInterceptorWithTracer(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "failed to get metadata")
	}
	reqId, ok := md[proto.ReqIdKey]
	if ok {
		_, ctx = trace.StartSpanFromContextWithTraceID(ctx, "", reqId[0])
	} else {
		_, ctx = trace.StartSpanFromContext(ctx, "")
	}

	resp, err = handler(ctx, req)
	return
}

func (r *RPCServer) unaryInterceptorWithAuditLog(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
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
		r.auditRecorder.Log(bw.Bytes())
	}

	return
}
