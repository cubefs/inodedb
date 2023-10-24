// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: service.proto

package raft

import (
	context "context"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

func init() { proto.RegisterFile("service.proto", fileDescriptor_a0b84a42fa06f626) }

var fileDescriptor_a0b84a42fa06f626 = []byte{
	// 163 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x4e, 0x2d, 0x2a,
	0xcb, 0x4c, 0x4e, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0xc9, 0xcc, 0xcb, 0x4f, 0x49,
	0x4d, 0x49, 0xd2, 0x2b, 0x4a, 0x4c, 0x2b, 0x91, 0xe2, 0x02, 0x91, 0x10, 0x19, 0xa3, 0x73, 0x8c,
	0x5c, 0xdc, 0x41, 0x89, 0x69, 0x25, 0xc1, 0x10, 0xf5, 0x42, 0x49, 0x5c, 0x02, 0x20, 0xae, 0x6f,
	0x6a, 0x71, 0x71, 0x62, 0x7a, 0xaa, 0x53, 0x62, 0x49, 0x72, 0x86, 0x90, 0xaa, 0x1e, 0xb2, 0x76,
	0x3d, 0x24, 0xf9, 0xa0, 0xd4, 0xc2, 0xd2, 0xd4, 0xe2, 0x12, 0xb0, 0x32, 0x29, 0x45, 0x3c, 0xca,
	0x8a, 0x0b, 0xf2, 0xf3, 0x8a, 0x53, 0x95, 0x18, 0x34, 0x18, 0x0d, 0x18, 0x85, 0xa2, 0xb9, 0x78,
	0xc0, 0x56, 0xe6, 0x25, 0x16, 0x14, 0x67, 0xe4, 0x97, 0x08, 0x61, 0xd1, 0x08, 0x93, 0x83, 0x5a,
	0x20, 0xa5, 0x84, 0x4f, 0x09, 0xb2, 0xe1, 0x4e, 0x9c, 0x51, 0xec, 0x7a, 0xfa, 0xd6, 0x20, 0x55,
	0x49, 0x6c, 0x60, 0x2f, 0x1a, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x89, 0x2c, 0xc2, 0x09, 0x0d,
	0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// RaftServiceClient is the client API for RaftService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type RaftServiceClient interface {
	RaftMessageBatch(ctx context.Context, opts ...grpc.CallOption) (RaftService_RaftMessageBatchClient, error)
	RaftSnapshot(ctx context.Context, opts ...grpc.CallOption) (RaftService_RaftSnapshotClient, error)
}

type raftServiceClient struct {
	cc *grpc.ClientConn
}

func NewRaftServiceClient(cc *grpc.ClientConn) RaftServiceClient {
	return &raftServiceClient{cc}
}

func (c *raftServiceClient) RaftMessageBatch(ctx context.Context, opts ...grpc.CallOption) (RaftService_RaftMessageBatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_RaftService_serviceDesc.Streams[0], "/inodedb.raft.RaftService/RaftMessageBatch", opts...)
	if err != nil {
		return nil, err
	}
	x := &raftServiceRaftMessageBatchClient{stream}
	return x, nil
}

type RaftService_RaftMessageBatchClient interface {
	Send(*RaftMessageRequestBatch) error
	Recv() (*RaftMessageResponse, error)
	grpc.ClientStream
}

type raftServiceRaftMessageBatchClient struct {
	grpc.ClientStream
}

func (x *raftServiceRaftMessageBatchClient) Send(m *RaftMessageRequestBatch) error {
	return x.ClientStream.SendMsg(m)
}

func (x *raftServiceRaftMessageBatchClient) Recv() (*RaftMessageResponse, error) {
	m := new(RaftMessageResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *raftServiceClient) RaftSnapshot(ctx context.Context, opts ...grpc.CallOption) (RaftService_RaftSnapshotClient, error) {
	stream, err := c.cc.NewStream(ctx, &_RaftService_serviceDesc.Streams[1], "/inodedb.raft.RaftService/RaftSnapshot", opts...)
	if err != nil {
		return nil, err
	}
	x := &raftServiceRaftSnapshotClient{stream}
	return x, nil
}

type RaftService_RaftSnapshotClient interface {
	Send(*RaftSnapshotRequest) error
	Recv() (*RaftSnapshotResponse, error)
	grpc.ClientStream
}

type raftServiceRaftSnapshotClient struct {
	grpc.ClientStream
}

func (x *raftServiceRaftSnapshotClient) Send(m *RaftSnapshotRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *raftServiceRaftSnapshotClient) Recv() (*RaftSnapshotResponse, error) {
	m := new(RaftSnapshotResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// RaftServiceServer is the server API for RaftService service.
type RaftServiceServer interface {
	RaftMessageBatch(RaftService_RaftMessageBatchServer) error
	RaftSnapshot(RaftService_RaftSnapshotServer) error
}

// UnimplementedRaftServiceServer can be embedded to have forward compatible implementations.
type UnimplementedRaftServiceServer struct {
}

func (*UnimplementedRaftServiceServer) RaftMessageBatch(srv RaftService_RaftMessageBatchServer) error {
	return status.Errorf(codes.Unimplemented, "method RaftMessageBatch not implemented")
}
func (*UnimplementedRaftServiceServer) RaftSnapshot(srv RaftService_RaftSnapshotServer) error {
	return status.Errorf(codes.Unimplemented, "method RaftSnapshot not implemented")
}

func RegisterRaftServiceServer(s *grpc.Server, srv RaftServiceServer) {
	s.RegisterService(&_RaftService_serviceDesc, srv)
}

func _RaftService_RaftMessageBatch_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RaftServiceServer).RaftMessageBatch(&raftServiceRaftMessageBatchServer{stream})
}

type RaftService_RaftMessageBatchServer interface {
	Send(*RaftMessageResponse) error
	Recv() (*RaftMessageRequestBatch, error)
	grpc.ServerStream
}

type raftServiceRaftMessageBatchServer struct {
	grpc.ServerStream
}

func (x *raftServiceRaftMessageBatchServer) Send(m *RaftMessageResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *raftServiceRaftMessageBatchServer) Recv() (*RaftMessageRequestBatch, error) {
	m := new(RaftMessageRequestBatch)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _RaftService_RaftSnapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(RaftServiceServer).RaftSnapshot(&raftServiceRaftSnapshotServer{stream})
}

type RaftService_RaftSnapshotServer interface {
	Send(*RaftSnapshotResponse) error
	Recv() (*RaftSnapshotRequest, error)
	grpc.ServerStream
}

type raftServiceRaftSnapshotServer struct {
	grpc.ServerStream
}

func (x *raftServiceRaftSnapshotServer) Send(m *RaftSnapshotResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *raftServiceRaftSnapshotServer) Recv() (*RaftSnapshotRequest, error) {
	m := new(RaftSnapshotRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _RaftService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "inodedb.raft.RaftService",
	HandlerType: (*RaftServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RaftMessageBatch",
			Handler:       _RaftService_RaftMessageBatch_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "RaftSnapshot",
			Handler:       _RaftService_RaftSnapshot_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "service.proto",
}
