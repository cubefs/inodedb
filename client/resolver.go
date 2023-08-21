package client

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/proto"

	"google.golang.org/grpc/resolver"
)

const (
	lbResolverSchema               = "static"
	serviceDiscoveryResolverSchema = "discovery"
)

func init() {
	resolver.Register(&LBBuilder{})
}

type LBBuilder struct{}

func (lb *LBBuilder) Build(target resolver.Target, cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error,
) {
	endpoints := strings.Split(target.Endpoint(), ",")

	r := &LBResolver{
		endpoints: endpoints,
		cc:        cc,
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (lb *LBBuilder) Scheme() string {
	return lbResolverSchema
}

type LBResolver struct {
	endpoints []string
	cc        resolver.ClientConn
}

func (lr *LBResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	var addresses []resolver.Address
	for i, addr := range lr.endpoints {
		addresses = append(addresses, resolver.Address{
			Addr:       addr,
			ServerName: fmt.Sprintf("instance-%d", i+1),
		})
	}

	newState := resolver.State{
		Addresses: addresses,
	}
	lr.cc.UpdateState(newState)
}

func (lr *LBResolver) Close() {}

type ServiceDiscoveryBuilder struct {
	masterClient *MasterClient
	role         proto.NodeRole
	timeout      time.Duration
}

func (sb *ServiceDiscoveryBuilder) Build(target resolver.Target, cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error,
) {
	r := &ServiceDiscoveryResolver{
		masterClient: sb.masterClient,
		role:         sb.role,
		cc:           cc,
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (sb *ServiceDiscoveryBuilder) Scheme() string {
	return serviceDiscoveryResolverSchema
}

type ServiceDiscoveryResolver struct {
	masterClient *MasterClient
	cc           resolver.ClientConn
	role         proto.NodeRole
	timeout      time.Duration
}

func (sr *ServiceDiscoveryResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	var addresses []resolver.Address
	// get router service list by master
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	ctx, _ = context.WithTimeout(ctx, sr.timeout)
	resp, err := sr.masterClient.GetRoleNodes(ctx, &proto.GetRoleNodesRequest{Role: sr.role})
	if err != nil {
		span.Warnf("get role nodes from master failed: %s", err)
		return
	}
	if len(resp.Nodes) == 0 {
		span.Warn("no role nodes found from master")
		return
	}
	for i, node := range resp.Nodes {
		addresses = append(addresses, resolver.Address{
			Addr:       node.Addr + strconv.Itoa(int(node.GrpcPort)),
			ServerName: fmt.Sprintf("role[%d]-instance-[%d]", sr.role, i+1),
		})
	}

	newState := resolver.State{
		Addresses: addresses,
	}
	sr.cc.UpdateState(newState)
}

func (sr *ServiceDiscoveryResolver) Close() {
	sr.masterClient.Close()
}
