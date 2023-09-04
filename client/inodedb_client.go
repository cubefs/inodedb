package client

type InodeDBConfig struct {
	RouterConfig
}

type InodeDBClient struct {
	*MasterClient
	*ShardServerClient
	*RouterClient
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

	cfg.RouterConfig.MasterClient = masterClient
	routerClient, err := NewRouterClient(&cfg.RouterConfig)
	if err != nil {
		return nil, err
	}

	return &InodeDBClient{
		MasterClient:      masterClient,
		ShardServerClient: shardServerClient,
		RouterClient:      routerClient,
	}, nil
}
