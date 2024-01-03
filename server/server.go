package server

import (
	"os"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/master"
	"github.com/cubefs/inodedb/master/base"
	"github.com/cubefs/inodedb/master/catalog"
	"github.com/cubefs/inodedb/master/cluster"
	masterStore "github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/router"
	"github.com/cubefs/inodedb/shardserver"
	sc "github.com/cubefs/inodedb/shardserver/catalog"
	shardServerStore "github.com/cubefs/inodedb/shardserver/store"
)

const (
	maxListNum = 1000
)

type Config struct {
	AuditLog auditlog.Config `json:"audit_log"`
	Roles    []string        `json:"roles"`

	// public config
	NodeConfig           proto.Node             `json:"node_config"`
	StoreConfig          StoreConfig            `json:"store_config"`
	MasterRpcConfig      client.MasterConfig    `json:"master_rpc_config"`
	ShardServerRpcConfig client.TransportConfig `json:"shard_server_rpc_config"`

	// shard server config
	Disks           []string           `json:"disks_config"`
	ShardRaftConfig raft.Config        `json:"shard_server_raft_config"`
	ShardBaseConfig sc.ShardBaseConfig `json:"shard_base_config"`

	// master config
	CatalogConfig catalog.Config   `json:"catalog_config"`
	ClusterConfig cluster.Config   `json:"cluster_config"`
	MasterRaftCfg base.RaftNodeCfg `json:"master_raft_cfg"`
}

type StoreConfig struct {
	Path       string         `json:"path"`
	KVOption   kvstore.Option `json:"kv_option"`
	RaftOption kvstore.Option `json:"raft_option"`
}

type Server struct {
	master      *master.Master
	router      *router.Router
	shardServer *shardserver.ShardServer

	initModuleHandler map[proto.NodeRole]func()
	auditRecorder     auditlog.LogCloser
	logHandler        rpc.ProgressHandler
	cfg               *Config
}

func NewServer(cfg *Config) *Server {
	// check log dir exist
	if _, err := os.Stat(cfg.AuditLog.LogDir); os.IsNotExist(err) {
		os.Mkdir(cfg.AuditLog.LogDir, 0o777)
	}

	logHandler, auditRecorder, err := auditlog.Open("inodedb", &cfg.AuditLog)
	if err != nil {
		log.Fatal("failed to open audit log:", err)
	}

	server := &Server{
		initModuleHandler: map[proto.NodeRole]func(){},
		auditRecorder:     auditRecorder,
		logHandler:        logHandler,
		cfg:               cfg,
	}

	initShardServer := func() {
		server.shardServer = shardserver.NewShardServer(&shardserver.Config{
			CatalogConfig: sc.Config{
				Disks: cfg.Disks,
				StoreConfig: shardServerStore.Config{
					KVOption:   cfg.StoreConfig.KVOption,
					RaftOption: cfg.StoreConfig.RaftOption,
				},
				MasterConfig:    cfg.MasterRpcConfig,
				NodeConfig:      cfg.NodeConfig,
				RaftConfig:      cfg.ShardRaftConfig,
				ShardBaseConfig: cfg.ShardBaseConfig,
			},
		})
	}
	initMaster := func() {
		server.master = master.NewMaster(&master.Config{
			StoreConfig: masterStore.Config{
				Path:       cfg.StoreConfig.Path + "/master/",
				KVOption:   cfg.StoreConfig.KVOption,
				RaftOption: cfg.StoreConfig.RaftOption,
			},
			CatalogConfig: cfg.CatalogConfig,
			ClusterConfig: cfg.ClusterConfig,
			RaftConfig:    cfg.MasterRaftCfg,
		})
	}
	initRouter := func() {
		server.router = router.NewRouter(&router.Config{
			ServerConfig: cfg.ShardServerRpcConfig,
			MasterConfig: cfg.MasterRpcConfig,
			NodeConfig:   cfg.NodeConfig,
		})
	}

	server.initModuleHandler = map[proto.NodeRole]func(){
		proto.NodeRole_ShardServer: initShardServer,
		proto.NodeRole_Master:      initMaster,
		proto.NodeRole_Router:      initRouter,
	}

	for _, role := range cfg.Roles {
		switch role {
		case proto.NodeRole_ShardServer.String():
			initShardServer()
		case proto.NodeRole_Master.String():
			initMaster()
		case proto.NodeRole_Router.String():
			initRouter()
		case proto.NodeRole_Single.String():
			initMaster()
			server.shardServer = &shardserver.ShardServer{}
			server.router = &router.Router{}
		default:
			log.Fatalf("unknown node role")
		}
	}

	return server
}

func (s *Server) Close() {
	s.auditRecorder.Close()
}
