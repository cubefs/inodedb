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
	"context"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultShutdownTimeoutS      = 10
	defaultReadRequestTimeoutS   = 30
	defaultWriteResponseTimeoutS = 30
)

type HttpServer struct {
	httpServer *http.Server

	*Server
}

func NewHttpServer(server *Server) *HttpServer {
	return &HttpServer{Server: server}
}

func (h *HttpServer) Serve(addr string) {
	ph := profile.NewProfileHandler(addr)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      rpc.MiddlewareHandlerWith(h.newHandler(), h.logHandler, ph),
		ReadTimeout:  defaultReadRequestTimeoutS * time.Second,
		WriteTimeout: defaultWriteResponseTimeoutS * time.Second,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("http server exits:", err)
		}
	}()
	h.httpServer = httpServer

	log.Info("http server is running at:", addr)
}

func (h *HttpServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeoutS*time.Second)
	defer cancel()

	h.httpServer.Shutdown(ctx)
}

func (h *HttpServer) newHandler() *rpc.Router {
	rpc.GET("/stats", h.Stats, rpc.OptArgsQuery())

	return rpc.DefaultRouter
}

func (h *HttpServer) Stats(c *rpc.Context) {
	c.RespondStatus(http.StatusOK)
}
