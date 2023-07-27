package metrics

import (
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Registry = prometheus.NewRegistry()

	GRPCMetrics = grpcprometheus.NewServerMetrics(
		func(c *prometheus.CounterOpts) {
			c.Namespace = "InodeDB"
		},
	)
)

func init() {
	Registry.MustRegister(
		GRPCMetrics,
	)
	GRPCMetrics.EnableHandlingTimeHistogram(
		func(h *prometheus.HistogramOpts) {
			h.Namespace = "InodeDB"
		},
	)
}
