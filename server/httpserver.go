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
