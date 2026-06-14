package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestTotal 请求总数
	// 按方法和状态码统计业务方请求
	RequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_request_total",
		Help: "Total number of requests from upstream",
	}, []string{"method", "status"})

	// RequestDuration 请求耗时
	// 按方法统计从业务方请求到响应的总耗时
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bridge_request_duration_seconds",
		Help:    "Request duration in seconds (upstream to downstream)",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	// ActiveConnections 活跃连接数
	// 当前与业务方建立的 gRPC 连接数
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_active_connections",
		Help: "Number of active connections from upstream",
	})
)
