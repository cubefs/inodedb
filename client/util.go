package client

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

const (
	defaultConnectionTimeoutMs = 100
	defaultMaxTimeoutMs        = 5000
	defaultBackoffMaxDelayMs   = 5000
	defaultBackoffBaseDelayMs  = 200
	defaultKeepAliveTimeoutS   = 60
)

func generateDialOpts(cfg *TransportConfig) []grpc.DialOption {
	if cfg.ConnectTimeoutMs == 0 {
		cfg.ConnectTimeoutMs = defaultConnectionTimeoutMs
	}
	if cfg.MaxTimeoutMs == 0 {
		cfg.MaxTimeoutMs = defaultMaxTimeoutMs
	}
	if cfg.KeepaliveTimeoutS == 0 {
		cfg.KeepaliveTimeoutS = defaultKeepAliveTimeoutS
	}
	if cfg.BackoffMaxDelayMs == 0 {
		cfg.BackoffMaxDelayMs = defaultBackoffMaxDelayMs
	}
	if cfg.BackoffBaseDelayMs == 0 {
		cfg.BackoffBaseDelayMs = defaultBackoffBaseDelayMs
	}

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
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return dialOpts
}

func unaryInterceptorWithTracer(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	span := trace.SpanFromContextSafe(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		proto.ReqIdKey, span.TraceID(),
	))

	return invoker(ctx, method, req, reply, cc, opts...)
}
