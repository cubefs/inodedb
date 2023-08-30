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
	"syscall"

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

	HttpBindAddr  string    `json:"http_bind_addr"`
	GRPCBindAddr  string    `json:"grpc_bind_addr"`
	MaxProcessors int       `json:"max_processors"`
	LogLevel      log.Level `json:"log_level"`
}

func main() {
	config.Init("f", "", "server.conf")

	cfg := &Config{}
	if err := config.Load(cfg); err != nil {
		log.Fatal(errors.Detail(err))
	}
	if cfg.MaxProcessors > 0 {
		runtime.GOMAXPROCS(cfg.MaxProcessors)
	}

	registerLogLevel()
	modifyOpenFiles()
	log.SetOutputLevel(cfg.LogLevel)

	startServer := server.NewServer(&cfg.Config)
	// start http server
	httpServer := server.NewHttpServer(startServer)
	httpServer.Serve(cfg.HttpBindAddr)

	// start grpc server
	grpcServer := server.NewRPCServer(startServer)
	grpcServer.Serve(cfg.GRPCBindAddr)

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
