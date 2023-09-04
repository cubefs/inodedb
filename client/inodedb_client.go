package client

type InodeDBConfig struct {
	ShardServerConfig
}

type InodeDBClient struct {
	*MasterClient
	*ShardServerClient
}

func NewInodeDBClient(cfg *InodeDBConfig) (*InodeDBClient, error) {
	masterClient, err := NewMasterClient(&MasterConfig{
		MasterAddresses: cfg.MasterAddresses,
		TransportConfig: cfg.TransportConfig,
	})
	if err != nil {
		return nil, err
	}

	shardServerClient, err := NewShardServerClient(&ShardServerConfig{
		TransportConfig: cfg.TransportConfig,
		MasterClient:    masterClient,
	})
	if err != nil {
		return nil, err
	}

	return &InodeDBClient{
		MasterClient:      masterClient,
		ShardServerClient: shardServerClient,
	}, nil
}
