package client

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
)

type (
	RouterConfig struct {
		RouterAddresses string          `json:"router_addresses"`
		MasterAddresses string          `json:"master_addresses"`
		TransportConfig TransportConfig `json:"transport"`
		MasterClient    *MasterClient   `json:"-"`
	}

	RouterClient struct {
		conn *grpc.ClientConn
		tc   TransportConfig

		proto.InodeDBRouterClient
	}
)

func NewRouterClient(cfg *RouterConfig) (*RouterClient, error) {
	if cfg.MasterAddresses == "" && cfg.RouterAddresses == "" && cfg.MasterClient == nil {
		return nil, errors.New("router and master address can't be nil both")
	}
	if cfg.MasterAddresses != "" && !strings.HasPrefix(cfg.MasterAddresses, lbResolverSchema+":///") {
		cfg.MasterAddresses = lbResolverSchema + ":///" + cfg.MasterAddresses
	}
	if cfg.RouterAddresses != "" && !strings.HasPrefix(cfg.RouterAddresses, lbResolverSchema+":///") {
		cfg.RouterAddresses = lbResolverSchema + ":///" + cfg.RouterAddresses
	}

	dialOpts := generateDialOpts(&cfg.TransportConfig)
	if cfg.RouterAddresses != "" {
		conn, err := grpc.Dial(cfg.RouterAddresses, dialOpts...)
		if err != nil {
			return nil, err
		}

		return &RouterClient{
			InodeDBRouterClient: proto.NewInodeDBRouterClient(conn),
			conn:                conn,
			tc:                  cfg.TransportConfig,
		}, nil
	}

	if cfg.MasterClient == nil {
		masterClient, err := NewMasterClient(&MasterConfig{
			MasterAddresses: cfg.MasterAddresses,
			TransportConfig: cfg.TransportConfig,
		})
		if err != nil {
			return nil, err
		}

		cfg.MasterClient = masterClient
	}

	// register master service discovery builder
	dialOpts = append(dialOpts, grpc.WithResolvers(&ServiceDiscoveryBuilder{
		masterClient: cfg.MasterClient,
		role:         proto.NodeRole_Router,
		timeout:      time.Millisecond * time.Duration(cfg.TransportConfig.MaxTimeoutMs),
	}))
	// get router service list by master
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(cfg.TransportConfig.MaxTimeoutMs))
	resp, err := cfg.MasterClient.GetRoleNodes(ctx, &proto.GetRoleNodesRequest{Role: proto.NodeRole_Router})
	if err != nil {
		return nil, err
	}
	if len(resp.Nodes) == 0 {
		return nil, errors.New("no role router nodes registered")
	}

	routerAddress := resp.Nodes[0].Addr + strconv.Itoa(int(resp.Nodes[0].GrpcPort))
	conn, err := grpc.Dial(routerAddress, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &RouterClient{
		InodeDBRouterClient: proto.NewInodeDBRouterClient(conn),
		conn:                conn,
		tc:                  cfg.TransportConfig,
	}, nil
}

func (c *RouterClient) Close() error {
	c.conn.Close()
	return nil
}
