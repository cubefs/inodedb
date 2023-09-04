package server

import (
	"os"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/master"
	"github.com/cubefs/inodedb/master/catalog"
	"github.com/cubefs/inodedb/master/cluster"
	masterStore "github.com/cubefs/inodedb/master/store"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/router"
	"github.com/cubefs/inodedb/shardserver"
	shardServerStore "github.com/cubefs/inodedb/shardserver/store"
)

const (
	maxListNum = 1000
)

type Config struct {
	AuditLog auditlog.Config `json:"audit_log"`
	Roles    []string        `json:"roles"`

	NodeConfig      proto.Node             `json:"node_config"`
	StoreConfig     StoreConfig            `json:"store_config"`
	MasterRpcConfig client.MasterConfig    `json:"master_rpc_config"`
	ServerRpcConfig client.TransportConfig `json:"server_rpc_config"`

	CatalogConfig catalog.Config `json:"catalog_config"`
	ClusterConfig cluster.Config `json:"cluster_config"`
}

type StoreConfig struct {
	Path     string         `json:"path"`
	KVOption kvstore.Option `json:"kv_option"`
}

type Server struct {
	master      *master.Master
	router      *router.Router
	shardServer *shardserver.ShardServer

	auditRecorder auditlog.LogCloser
	logHandler    rpc.ProgressHandler
	cfg           *Config
}

func NewServer(cfg *Config) *Server {
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	// check log dir exist
	if _, err := os.Stat(cfg.AuditLog.LogDir); os.IsNotExist(err) {
		os.Mkdir(cfg.AuditLog.LogDir, 0o777)
	}

	logHandler, auditRecorder, err := auditlog.Open("inodedb", &cfg.AuditLog)
	if err != nil {
		log.Fatal("failed to open audit log:", err)
	}

	server := &Server{
		auditRecorder: auditRecorder,
		logHandler:    logHandler,
		cfg:           cfg,
	}

	newShardServer := func() *shardserver.ShardServer {
		return shardserver.NewShardServer(&shardserver.Config{
			StoreConfig: shardServerStore.Config{
				Path:     cfg.StoreConfig.Path + "/shardserver/",
				KVOption: cfg.StoreConfig.KVOption,
			},
			MasterConfig: cfg.MasterRpcConfig,
			NodeConfig:   cfg.NodeConfig,
		})
	}

	newMaster := func() *master.Master {
		return master.NewMaster(&master.Config{
			StoreConfig: masterStore.Config{
				Path:     cfg.StoreConfig.Path + "/master/",
				KVOption: cfg.StoreConfig.KVOption,
			},
			CatalogConfig: cfg.CatalogConfig,
			ClusterConfig: cfg.ClusterConfig,
		})
	}

	newRouter := func() *router.Router {
		return router.NewRouter(&router.Config{
			ServerConfig: cfg.ServerRpcConfig,
			MasterConfig: cfg.MasterRpcConfig,
			NodeConfig:   cfg.NodeConfig,
		})
	}

	for _, role := range cfg.Roles {
		switch role {
		case proto.NodeRole_ShardServer.String():
			server.shardServer = newShardServer()
		case proto.NodeRole_Master.String():
			server.master = newMaster()
		case proto.NodeRole_Router.String():
			server.router = newRouter()
		case proto.NodeRole_Single.String():
			server.master = newMaster()
			server.shardServer = &shardserver.ShardServer{}
			// server.router = &router.Router{}
			// server.shardServer = newShardServer()
			// server.router = newRouter()
		default:
			log.Fatalf("unknown node role")
		}
	}

	return server
}

func (s *Server) Close() {
	s.auditRecorder.Close()
}
