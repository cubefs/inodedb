// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	AuditLog auditlog.Config  `json:"auditlog"`
	Roles    []proto.NodeRole `json:"roles"`

	MasterRpcConfig client.MasterConfig    `json:"master_rpc_config"`
	NodeConfig      proto.Node             `json:"node_config"`
	StoreConfig     StoreConfig            `json:"store_config"`
	ServerConfig    client.TransportConfig `json:"server_config"`

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
		auditRecorder: auditRecorder,
		logHandler:    logHandler,
	}

	shardServer := shardserver.NewShardServer(&shardserver.Config{
		StoreConfig: shardServerStore.Config{
			Path:     cfg.StoreConfig.Path + "/shardserver/",
			KVOption: cfg.StoreConfig.KVOption,
		},
		MasterConfig: cfg.MasterRpcConfig,
		NodeConfig:   cfg.NodeConfig,
	})

	master := master.NewMaster(&master.Config{
		StoreConfig: masterStore.Config{
			Path:     cfg.StoreConfig.Path + "/master/",
			KVOption: cfg.StoreConfig.KVOption,
		},
		CatalogConfig: cfg.CatalogConfig,
		ClusterConfig: cfg.ClusterConfig,
	})

	newRouter := router.NewRouter(&router.Config{
		ServerConfig: &cfg.ServerConfig,
		MasterConfig: &cfg.MasterRpcConfig,
		NodeConfig:   &cfg.NodeConfig,
	})

	for _, role := range cfg.Roles {
		switch role {
		case proto.NodeRole_ShardServer:
			server.shardServer = shardServer
		case proto.NodeRole_Master:
			server.master = master
		case proto.NodeRole_Router:
			server.router = newRouter
		case proto.NodeRole_Single:
			server.shardServer = shardServer
			server.master = master
			server.router = newRouter
		default:
			continue
		}
	}

	return server
}

func (s *Server) Close() {
	s.auditRecorder.Close()
}
