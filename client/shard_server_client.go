package client

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/cubefs/inodedb/proto"
	"golang.org/x/sync/singleflight"
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
		// diskNodes maintains the reflection of disk id to node id
		diskNodes sync.Map
		// catalog maintains space/shard info of the cluster
		catalog      sync.Map
		masterClient *MasterClient
		tc           *TransportConfig
		dialOpts     []grpc.DialOption
		sf           singleflight.Group
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

func (s *ShardServerClient) GetShardServerClient(ctx context.Context, diskID proto.DiskID) (proto.InodeDBShardServerClient, error) {
	v, ok := s.diskNodes.Load(diskID)
	if !ok {
		//  refresh shard server disks
		if _, err, _ := s.sf.Do("refresh-disk", func() (interface{}, error) {
			if err := s.refreshShardDisks(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		}); err != nil {
			return nil, err
		}
	}

	nodeID := v.(proto.NodeID)
	client, ok := s.shardServerClients.Load(nodeID)
	if !ok {
		// rebuild shard server client
		if _, err, _ := s.sf.Do("refresh-server", func() (interface{}, error) {
			if err := s.refreshShardServerClients(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		}); err != nil {
			return nil, err
		}
	}

	client, ok = s.shardServerClients.Load(nodeID)
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
		return errors.Info(err, "get role nodes failed")
	}
	if len(resp.Nodes) == 0 {
		return errors.New("no role shard server nodes registered")
	}

	for _, node := range resp.Nodes {
		if _, ok := s.shardServerClients.Load(node.ID); ok {
			if _, ok := s.shardServerClients.Load(node.ID); ok {
				continue
			}

			address := node.Addr + ":" + strconv.Itoa(int(node.GrpcPort))
			conn, err := grpc.Dial(address, s.dialOpts...)
			if err != nil {
				return errors.Info(err, "dial shard server failed", address)
			}

			shardServer := &shardServer{
				conn:                     conn,
				InodeDBShardServerClient: proto.NewInodeDBShardServerClient(conn),
			}
			s.shardServerClients.Store(node.ID, shardServer)
		}
	}
	return nil
}

func (s *ShardServerClient) refreshShardDisks(ctx context.Context) error {
	marker := uint32(0)
	for {
		resp, err := s.masterClient.ListDisks(ctx, &proto.ListDiskRequest{
			Marker: marker,
			Count:  1000,
		})
		if err != nil {
			return err
		}
		if len(resp.Disks) == 0 {
			break
		}

		for i := range resp.Disks {
			s.diskNodes.LoadOrStore(resp.Disks[i].DiskID, resp.Disks[i].NodeID)
		}
		marker = resp.Marker
	}

	return nil
}
