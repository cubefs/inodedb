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

package main

import (
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"

	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"

	"github.com/cubefs/inodedb/util"

	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	_ "github.com/cubefs/cubefs/blobstore/util/version"
	"github.com/cubefs/inodedb/server"
)

// Config service config
type Config struct {
	server.Config

	HttpBindPort  uint32    `json:"http_bind_port"`
	GrpcBindPort  uint32    `json:"grpc_bind_port"`
	MaxProcessors int       `json:"max_processors"`
	LogLevel      log.Level `json:"log_level"`
}

func main() {
	config.Init("f", "", "server.json")

	cfg := &Config{}
	if err := config.Load(cfg); err != nil {
		log.Fatal(errors.Detail(err))
	}

	initConfig(cfg)
	registerLogLevel()
	modifyOpenFiles()
	log.SetOutputLevel(cfg.LogLevel)

	startServer := server.NewServer(&cfg.Config)
	// start http server
	httpServer := server.NewHttpServer(startServer)
	httpServer.Serve(":" + strconv.Itoa(int(cfg.HttpBindPort)))

	// start grpc server
	grpcServer := server.NewRPCServer(startServer)
	grpcServer.Serve(":" + strconv.Itoa(int(cfg.GrpcBindPort)))

	// wait for signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch

	// stop all server
	grpcServer.Stop()
	httpServer.Stop()
	startServer.Close()
}

func registerLogLevel() {
	logLevelPath, logLevelHandler := log.ChangeDefaultLevelHandler()
	profile.HandleFunc(http.MethodPost, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
	profile.HandleFunc(http.MethodGet, logLevelPath, func(c *rpc.Context) {
		logLevelHandler.ServeHTTP(c.Writer, c.Request)
	})
}

func modifyOpenFiles() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Fatalf("getting rlimit failed: %s", err)
	}
	log.Info("system limit: ", rLimit)

	if rLimit.Cur >= 102400 && rLimit.Max >= 102400 {
		return
	}

	rLimit.Cur = 1024000
	rLimit.Max = 1024000

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Fatalf("setting rlimit faield: %s", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Fatalf("getting rlimit failed: %s", err)
	}
	log.Info("system limit: ", rLimit)
	return
}

func initConfig(cfg *Config) {
	cfg.NodeConfig.GrpcPort = cfg.GrpcBindPort
	cfg.NodeConfig.HttpPort = cfg.HttpBindPort
	cfg.ClusterConfig.GrpcPort = cfg.GrpcBindPort
	cfg.ClusterConfig.HttpPort = cfg.HttpBindPort

	if cfg.AuditLog.LogDir == "" {
		cfg.AuditLog.LogDir = "./run/audit_log"
	}
	if cfg.StoreConfig.Path == "" {
		cfg.StoreConfig.Path = "./run/store"
	}
	if cfg.MaxProcessors > 0 {
		runtime.GOMAXPROCS(cfg.MaxProcessors)
	}

	if cfg.NodeConfig.Addr == "" {
		var err error
		cfg.NodeConfig.Addr, err = util.GetLocalIP()
		if err != nil {
			log.Fatalf("can't get local ip address, please set the ip address for the node config")
		}
	}

	if len(cfg.ClusterConfig.Azs) == 0 {
		cfg.ClusterConfig.Azs = []string{"z0"}
	}
	cfg.CatalogConfig.AZs = cfg.ClusterConfig.Azs
	if cfg.NodeConfig.Az == "" {
		cfg.NodeConfig.Az = cfg.ClusterConfig.Azs[0]
	}

	if len(cfg.Roles) == 0 {
		log.Fatalf("node roles must be set")
	}
InitRoles:
	for _, role := range cfg.Roles {
		switch role {
		case proto.NodeRole_Single.String():
			cfg.NodeConfig.Roles = []proto.NodeRole{proto.NodeRole_ShardServer, proto.NodeRole_Router, proto.NodeRole_Master}
			cfg.CatalogConfig.ShardReplicateNum = 1
			cfg.MasterRpcConfig.MasterAddresses = "127.0.0.1:" + strconv.Itoa(int(cfg.GrpcBindPort))
			cfg.ClusterConfig.ClusterId = 1

			cfg.MasterRaftCfg.Members = []raft.Member{{NodeID: 1, Host: "127.0.0.1"}}
			cfg.MasterRaftCfg.RaftConfig.NodeID = 1
			
			break InitRoles
		case proto.NodeRole_Master.String():
			cfg.NodeConfig.Roles = append(cfg.NodeConfig.Roles, proto.NodeRole_Master)
		case proto.NodeRole_Router.String():
			cfg.NodeConfig.Roles = append(cfg.NodeConfig.Roles, proto.NodeRole_Router)
		case proto.NodeRole_ShardServer.String():
			cfg.NodeConfig.Roles = append(cfg.NodeConfig.Roles, proto.NodeRole_ShardServer)
		}
	}
}
