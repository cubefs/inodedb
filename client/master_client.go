package client

import (
	"errors"
	"strings"

	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
)

type (
	MasterConfig struct {
		MasterAddresses string          `json:"master_addresses"`
		TransportConfig TransportConfig `json:"transport"`
	}
	TransportConfig struct {
		MaxTimeoutMs       uint32 `json:"max_timeout_ms"`
		ConnectTimeoutMs   uint32 `json:"connect_timeout_ms"`
		KeepaliveTimeoutS  uint32 `json:"keepalive_timeout_s"`
		BackoffBaseDelayMs uint32 `json:"backoff_base_delay_ms"`
		BackoffMaxDelayMs  uint32 `json:"backoff_max_delay_ms"`
	}

	MasterClient struct {
		conn *grpc.ClientConn
		tc   TransportConfig

		proto.InodeDBMasterClient
	}
)

func NewMasterClient(cfg *MasterConfig) (*MasterClient, error) {
	if cfg.MasterAddresses == "" {
		return nil, errors.New("master address can't be nil")
	}
	if !strings.HasPrefix(cfg.MasterAddresses, lbResolverSchema+":///") {
		cfg.MasterAddresses = lbResolverSchema + ":///" + cfg.MasterAddresses
	}

	conn, err := grpc.Dial(cfg.MasterAddresses, generateDialOpts(&cfg.TransportConfig)...)
	if err != nil {
		return nil, err
	}
	client := proto.NewInodeDBMasterClient(conn)

	return &MasterClient{
		InodeDBMasterClient: client,
		conn:                conn,
	}, nil
}

func (c *MasterClient) Close() error {
	c.conn.Close()
	return nil
}
