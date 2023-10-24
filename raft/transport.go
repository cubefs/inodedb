package raft

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type connectionClass int8

const (
	// DefaultClass is the default ConnectionClass and should be used for most
	// client traffic.
	defaultConnectionClass connectionClass = iota
	// SystemClass is the ConnectionClass used for system traffic like heartbeat and so on.
	systemConnectionClass
	// NumConnectionClasses is the number of valid connectionClass values.
	numConnectionClasses
)

const (
	raftSendBufferSize = 1024

	defaultInflightMsgSize     = 64
	defaultConnectionTimeoutMs = 100
	defaultMaxTimeoutMs        = 5000
	defaultBackoffMaxDelayMs   = 5000
	defaultBackoffBaseDelayMs  = 200
	defaultKeepAliveTimeoutS   = 60
)

type (
	transportHandler interface {
		// HandleRaftRequest handle incoming raft request
		HandleRaftRequest(ctx context.Context, req *RaftMessageRequest, stream MessageResponseStream) error
		// HandleRaftResponse handle with raft response error
		HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error
		// HandleRaftSnapshot handle with raft snapshot request
		HandleRaftSnapshot(ctx context.Context, req *RaftSnapshotRequest, stream SnapshotResponseStream) error
	}
	TransportConfig struct {
		MaxInflightMsgSize int    `json:"max_inflight_msg_size"`
		MaxTimeoutMs       uint32 `json:"max_timeout_ms"`
		ConnectTimeoutMs   uint32 `json:"connect_timeout_ms"`
		KeepaliveTimeoutS  uint32 `json:"keepalive_timeout_s"`
		BackoffBaseDelayMs uint32 `json:"backoff_base_delay_ms"`
		BackoffMaxDelayMs  uint32 `json:"backoff_max_delay_ms"`

		Resolver AddressResolver  `json:"-"`
		Handler  transportHandler `json:"-"`
	}
)

func newTransport(cfg *TransportConfig) *transport {
	initialDefaultConfig(&cfg.MaxInflightMsgSize, defaultInflightMsgSize)
	initialDefaultConfig(&cfg.ConnectTimeoutMs, defaultConnectionTimeoutMs)
	initialDefaultConfig(&cfg.MaxTimeoutMs, defaultMaxTimeoutMs)
	initialDefaultConfig(&cfg.KeepaliveTimeoutS, defaultKeepAliveTimeoutS)
	initialDefaultConfig(&cfg.BackoffBaseDelayMs, defaultBackoffBaseDelayMs)
	initialDefaultConfig(&cfg.BackoffMaxDelayMs, defaultBackoffMaxDelayMs)

	t := &transport{
		resolver: cfg.Resolver,
		handler:  cfg.Handler,
		cfg:      cfg,
		done:     make(chan struct{}),
	}

	// register raft service
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(t.unaryInterceptorWithTracer))
	RegisterRaftServiceServer(s, t)

	return t
}

type transport struct {
	queues   [numConnectionClasses]sync.Map
	resolver AddressResolver
	handler  transportHandler
	conns    sync.Map

	cfg  *TransportConfig
	done chan struct{}
}

func (t *transport) RaftMessageBatch(stream RaftService_RaftMessageBatchServer) error {
	errCh := make(chan error, 1)

	go func(ctx context.Context) {
		errCh <- func() error {
			stream := &lockedMessageResponseStream{wrapped: stream}
			for {
				batch, err := stream.Recv()
				if err != nil {
					return err
				}
				if len(batch.Requests) == 0 {
					continue
				}

				for i := range batch.Requests {
					req := &batch.Requests[i]
					if err := t.handler.HandleRaftRequest(ctx, req, stream); err != nil {
						// todo: log error
						if err := stream.Send(newRaftMessageResponse(req, ErrGroupHandleRaftMessage)); err != nil {
							return err
						}
					}
				}
			}
		}()
	}(stream.Context())

	select {
	case <-t.done:
		return nil
	case err := <-errCh:
		// todo: log error
		return err
	}
}

func (t *transport) RaftSnapshot(stream RaftService_RaftSnapshotServer) error {
	errCh := make(chan error, 1)
	go func(ctx context.Context) {
		span := trace.SpanFromContextSafe(ctx)

		errCh <- func() error {
			req, err := stream.Recv()
			if err != nil {
				return err
			}
			if req.Header == nil {
				return stream.Send(&RaftSnapshotResponse{
					Status:  RaftSnapshotResponse_ERROR,
					Message: "snapshot sender error: no header in the first snapshot request",
				})
			}

			if err := t.handler.HandleRaftSnapshot(ctx, req, stream); err != nil {
				span.Errorf("handle raft snapshot failed: %s", err)
			}
			return err
		}()
	}(stream.Context())

	select {
	case <-t.done:
		return nil
	case err := <-errCh:
		// todo: log error
		return err
	}
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *transport) SendAsync(ctx context.Context, req *RaftMessageRequest, class connectionClass) error {
	toNodeID := req.To
	if req.GroupID == 0 && len(req.Heartbeats) == 0 && len(req.HeartbeatResponses) == 0 {
		// Coalesced heartbeats are addressed to range 0; everything else
		// needs an explicit range ID.
		panic("only messages with coalesced heartbeats or heartbeat responses may be sent to range ID 0")
	}

	// resolve address from to node id
	addr, err := t.resolver.Resolve(toNodeID)
	if err != nil {
		return fmt.Errorf("can't resolve to node id[%d], err: %s", toNodeID, err)
	}

	ch, existingQueue := t.getQueue(addr.String(), class)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		_, ctx := trace.StartSpanFromContext(context.Background(), "")
		go t.startProcessNewQueue(ctx, toNodeID, addr.String(), class)
	}

	select {
	case ch <- req:
		return nil
	default:
		return fmt.Errorf("send request into group queue failed, queue is full")
	}
}

func (t *transport) SendSnapshot(ctx context.Context, snapshot *outgoingSnapshot) error {
	span := trace.SpanFromContext(ctx)
	toNodeID := snapshot.RaftMessageRequest.To
	addr, err := t.resolver.Resolve(toNodeID)
	if err != nil {
		return fmt.Errorf("can't resolve to node id[%d]", toNodeID)
	}

	conn, err := t.getConnection(ctx, addr.String(), defaultConnectionClass)
	if err != nil {
		return err
	}

	client := NewRaftServiceClient(conn.ClientConn)
	stream, err := client.RaftSnapshot(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			span.Warnf("failed to close outgoingSnapshot stream: %s", err)
		}
	}()

	if err := snapshot.Send(ctx, stream); err != nil {
		return err
	}

	return nil
}

func (t *transport) Close() {
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *transport) getQueue(
	addr string, class connectionClass,
) (chan *RaftMessageRequest, bool) {
	queuesMap := &t.queues[class]
	value, ok := queuesMap.Load(addr)
	if !ok {
		ch := make(chan *RaftMessageRequest, raftSendBufferSize)
		value, ok = queuesMap.LoadOrStore(addr, ch)
	}
	return value.(chan *RaftMessageRequest), ok
}

func (t *transport) deleteQueue(addr string, class connectionClass) {
	queuesMap := &t.queues[class]
	queuesMap.Delete(addr)
}

// startProcessNewQueue connects to the node and launches a worker goroutine
// that processes the queue for the given nodeID (which must exist) until
// the underlying connection is closed or an error occurs. This method
// takes on the responsibility of deleting the queue when the worker shuts down.
// The class parameter dictates the ConnectionClass which should be used to dial
// the remote node. Traffic for system ranges and heartbeats will receive a
// different class than that of user data ranges.
//
// Returns whether the worker was started (the queue is deleted either way).
func (t *transport) startProcessNewQueue(
	ctx context.Context,
	toNodeID uint64,
	addr string,
	class connectionClass,
) {
	span := trace.SpanFromContext(ctx)

	ch, existingQueue := t.getQueue(addr, class)
	if !existingQueue {
		span.Fatalf("queue[%s] does not exist", addr)
	}
	defer t.queues[class].Delete(addr)

	conn, err := t.getConnection(ctx, addr, class)
	if err != nil {
		span.Warnf("get connection for node[%d] failed: %s", toNodeID, err)
		return
	}
	client := NewRaftServiceClient(conn.ClientConn)
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.RaftMessageBatch(batchCtx) // closed via cancellation
	if err != nil {
		span.Warnf("create raft message batch client for node[%d] failed: %s", toNodeID, err)
		return
	}

	if err := t.processQueue(ch, stream); err != nil {
		span.Warnf("processing raft message queue for node[%d] failed: %s", toNodeID, err)
	}
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *transport) processQueue(
	ch chan *RaftMessageRequest,
	stream RaftService_RaftMessageBatchClient,
) error {
	errCh := make(chan error, 1)

	ctx := stream.Context()
	go func(ctx context.Context) {
		errCh <- func() error {
			for {
				resp, err := stream.Recv()
				if err != nil {
					return err
				}

				if err := t.handler.HandleRaftResponse(ctx, resp); err != nil {
					return err
				}
			}
		}()
	}(ctx)

	batch := &RaftMessageRequestBatch{}
	for {
		select {
		case err := <-errCh:
			return err
		case req := <-ch:
			budget := t.cfg.MaxInflightMsgSize
			batch.Requests = append(batch.Requests, *req)
			req.release()
			// pull off as many queued requests as possible
			for budget > 0 {
				select {
				case req = <-ch:
					budget -= req.Size()
					batch.Requests = append(batch.Requests, *req)
				default:
					budget = -1
				}
			}

			err := stream.Send(batch)
			if err != nil {
				return err
			}

			// reuse the Requests slice, zero out the contents to avoid delaying
			// GC of memory referenced from within.
			for i := range batch.Requests {
				batch.Requests[i] = RaftMessageRequest{}
			}
			batch.Requests = batch.Requests[:0]
		}
	}
}

// unaryInterceptorWithTracer intercept incoming request with trace id
func (t *transport) unaryInterceptorWithTracer(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
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

func (t *transport) getConnection(ctx context.Context, target string, class connectionClass) (conn *connection, err error) {
	type thisKey struct {
		target string
		class  connectionClass
	}

	key := thisKey{
		target: target,
		class:  class,
	}

	value, loaded := t.conns.Load(key)
	if !loaded {
		value, _ = t.conns.LoadOrStore(key, &connection{})
	}
	conn = value.(*connection)

	if conn.ClientConn == nil {
		conn.once.Do(func() {
			grpcConn, dialErr := grpc.DialContext(ctx, target, generateDialOpts(t.cfg)...)
			if dialErr != nil {
				err = dialErr
				t.conns.Delete(target)
				return
			}
			grpcConn.Connect()
			conn.ClientConn = grpcConn
		})
	}

	return
}

type connection struct {
	*grpc.ClientConn

	once sync.Once
}

// MessageResponseStream is the subset of the
// RaftService_RaftMessageBatchServer interface that is needed for sending responses.
type MessageResponseStream interface {
	Context() context.Context
	Send(*RaftMessageResponse) error
}

// SnapshotResponseStream is the subset of the
// RaftService_RaftSnapshotServer interface that is needed for sending responses.
type SnapshotResponseStream interface {
	Context() context.Context
	Send(response *RaftSnapshotResponse) error
	Recv() (*RaftSnapshotRequest, error)
}

// lockedMessageResponseStream is an implementation of
// MessageResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedMessageResponseStream struct {
	wrapped RaftService_RaftMessageBatchServer
	sendMu  sync.Mutex
}

func (s *lockedMessageResponseStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedMessageResponseStream) Send(resp *RaftMessageResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(resp)
}

func (s *lockedMessageResponseStream) Recv() (*RaftMessageRequestBatch, error) {
	return s.wrapped.Recv()
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(req *RaftMessageRequest, err *Error) *RaftMessageResponse {
	resp := &RaftMessageResponse{
		GroupID: req.GroupID,
		To:      req.From,
		From:    req.To,
	}
	if err != nil {
		resp.Err = err
	}
	return resp
}

// generateDialOpts generate grpc dial options
func generateDialOpts(cfg *TransportConfig) []grpc.DialOption {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Timeout:             time.Duration(cfg.KeepaliveTimeoutS) * time.Second,
				PermitWithoutStream: true,
			},
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay: time.Duration(cfg.BackoffBaseDelayMs) * time.Millisecond,
				MaxDelay:  time.Duration(cfg.BackoffMaxDelayMs) * time.Millisecond,
			},
			MinConnectTimeout: time.Millisecond * time.Duration(cfg.ConnectTimeoutMs),
		}),
		grpc.WithChainUnaryInterceptor(unaryInterceptorWithTracer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return dialOpts
}

// unaryInterceptorWithTracer intercept client request with trace id
func unaryInterceptorWithTracer(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	span := trace.SpanFromContextSafe(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		proto.ReqIdKey, span.TraceID(),
	))

	return invoker(ctx, method, req, reply, cc, opts...)
}
