package client

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
)

type (
	ShardServerConfig struct {
		MasterAddresses string          `json:"master_addresses"`
		TransportConfig TransportConfig `json:"transport"`
		MasterClient    *MasterClient   `json:"-"`
	}

	ShardServerClient struct {
		// shardServerClients maintains grpc client by node id
		shardServerClients sync.Map
		// catalog maintains space/shard info of the cluster
		catalog      sync.Map
		masterClient *MasterClient
		tc           *TransportConfig
		dialOpts     []grpc.DialOption
	}
	shardServer struct {
		conn *grpc.ClientConn
		tc   *TransportConfig

		proto.InodeDBShardServerClient
	}
)

func NewShardServerClient(cfg *ShardServerConfig) (*ShardServerClient, error) {
	if cfg.MasterAddresses == "" && cfg.MasterClient == nil {
		return nil, errors.New("master address and master client can't be nil both")
	}
	if cfg.MasterAddresses != "" && !strings.HasPrefix(cfg.MasterAddresses, lbResolverSchema+":///") {
		cfg.MasterAddresses = lbResolverSchema + ":///" + cfg.MasterAddresses
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
	shardServerClient := &ShardServerClient{
		masterClient: cfg.MasterClient,
		dialOpts:     generateDialOpts(&cfg.TransportConfig),
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(cfg.TransportConfig.MaxTimeoutMs))
	if err := shardServerClient.refreshShardServerClients(ctx); err != nil {
		return nil, err
	}

	return shardServerClient, nil
}

func (s *ShardServerClient) GetClient(ctx context.Context, nodeId uint32) (proto.InodeDBShardServerClient, error) {
	client, ok := s.shardServerClients.Load(nodeId)
	if !ok {
		// rebuild shard server client
		if err := s.refreshShardServerClients(ctx); err != nil {
			return nil, err
		}
	}

	client, ok = s.shardServerClients.Load(nodeId)
	if !ok {
		return nil, errors.New("shard server not found")
	}
	return client.(*shardServer), nil
}

func (s *ShardServerClient) Close() error {
	s.shardServerClients.Range(func(key, value interface{}) bool {
		value.(*shardServer).conn.Close()
		return true
	})
	return nil
}

func (s *ShardServerClient) refreshShardServerClients(ctx context.Context) error {
	// get all shard server list by master and build shard server clients
	resp, err := s.masterClient.GetRoleNodes(ctx, &proto.GetRoleNodesRequest{Role: proto.NodeRole_ShardServer})
	if err != nil {
		return err
	}
	if len(resp.Nodes) == 0 {
		return errors.New("no role shard server nodes registered")
	}

	for _, node := range resp.Nodes {
		if _, ok := s.shardServerClients.Load(node.Id); ok {
			continue
		}

		routerAddress := node.Addr + strconv.Itoa(int(node.GrpcPort))
		conn, err := grpc.Dial(routerAddress, s.dialOpts...)
		if err != nil {
			return err
		}

		shardServer := &shardServer{
			conn:                     conn,
			InodeDBShardServerClient: proto.NewInodeDBShardServerClient(conn),
		}
		s.shardServerClients.Store(node.Id, shardServer)
	}

	return nil
}
