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
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/master"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/router"
	"github.com/cubefs/inodedb/shardserver"
	"github.com/cubefs/inodedb/shardserver/store"
)

const (
	maxListNum = 1000
)

type Config struct {
	Role []proto.NodeRole

	MasterConfig client.MasterConfig `json:"master_config"`
	NodeConfig   proto.Node          `json:"node_config"`
	StoreConfig  store.Config        `json:"store_config"`
}

type Server struct {
	master           *master.Master
	router           *router.Router
	shardServer      *shardserver.ShardServer
	auditLogRecorder map[proto.NodeRole]auditlog.LogCloser
}

func NewServer(cfg *Config) *Server {
	shardServer := shardserver.NewShardServer(&shardserver.Config{
		StoreConfig:  cfg.StoreConfig,
		MasterConfig: cfg.MasterConfig,
		NodeConfig:   cfg.NodeConfig,
	})
	return &Server{shardServer: shardServer}
}
