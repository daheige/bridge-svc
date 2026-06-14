package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	bridgev1 "github.com/daheige/bridge-svc/api/v1"
	"github.com/daheige/bridge-svc/internal/config"
	"github.com/daheige/bridge-svc/internal/middleware"
	"github.com/daheige/bridge-svc/internal/observability"
	"github.com/daheige/bridge-svc/internal/protocol"
	"github.com/daheige/bridge-svc/internal/resilience"
	"github.com/daheige/bridge-svc/internal/router"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// BridgeServer Bridge 服务实现
// 对外暴露 gRPC 接口，接收业务方请求，转发到下游微服务
type BridgeServer struct {
	bridgev1.UnimplementedBridgeServiceServer

	router     *router.Router                    // 路由引擎
	breakerMgr *resilience.CircuitBreakerManager // 熔断器管理
	cfg        *config.Config                    // 配置
}

// New 创建 BridgeServer 实例
func New(cfg *config.Config) (*BridgeServer, error) {
	r, err := router.New(&cfg.Etcd)
	if err != nil {
		return nil, fmt.Errorf("init router: %w", err)
	}

	return &BridgeServer{
		router:     r,
		breakerMgr: resilience.NewCircuitBreakerManager(cfg.Resilience.CircuitBreaker),
		cfg:        cfg,
	}, nil
}

// CallUnary 一元调用处理
// 接收业务方的 gRPC 请求，路由到下游微服务并返回响应
func (s *BridgeServer) CallUnary(ctx context.Context, req *bridgev1.UnaryRequest) (*bridgev1.UnaryResponse, error) {
	start := time.Now()

	// 1. 路由决策：根据 target 找到下游微服务节点
	routeCtx := router.RouteContext{
		Target:   req.Target,
		Protocol: router.ProtocolType(req.Protocol),
		Metadata: toMetadataMD(req.Metadata),
	}

	// 根据 target 找到对应的下游微服务节点
	target, err := s.router.Route(ctx, routeCtx)
	// fmt.Printf("\ntarget: %v err: %v\n", target, err)
	if err != nil {
		return &bridgev1.UnaryResponse{
			Status: status.New(codes.Unavailable, fmt.Sprintf("routing failed: %v", err)).Proto(),
		}, nil
	}

	// 2. 计算超时：业务方指定 > 配置默认值
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = s.cfg.Protocols.DefaultTimeout
	}

	// 3. 独立超时上下文（避免上游 deadline 干扰熔断器判断）
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 4. 获取协议处理器（根据下游微服务协议类型）
	handler := protocol.Factory(target.Endpoint.Protocol)
	if handler == nil {
		return &bridgev1.UnaryResponse{
			Status:       status.New(codes.InvalidArgument, fmt.Sprintf("unsupported protocol: %s", req.Protocol)).Proto(),
			UpstreamNode: target.Endpoint.Address,
		}, nil
	}

	// 5. 熔断保护执行：在熔断器保护下调用下游微服务
	result, err := s.breakerMgr.Execute(target.Endpoint.Address, func() (interface{}, error) {
		return handler.Call(callCtx, target, req.Payload, toMetadataMD(req.Metadata), timeout)
	})

	latency := uint64(time.Since(start).Microseconds())

	if err != nil {
		statusCode := codes.Unavailable
		if errors.Is(err, resilience.ErrCircuitOpen) {
			statusCode = codes.Unavailable
		}

		return &bridgev1.UnaryResponse{
			Status:       status.New(statusCode, err.Error()).Proto(),
			UpstreamNode: target.Endpoint.Address,
			LatencyUs:    latency,
		}, nil
	}

	resp := result.(*protocol.Response)
	return &bridgev1.UnaryResponse{
		Payload:      resp.Payload,
		Metadata:     mdToMap(resp.Metadata),
		UpstreamNode: target.Endpoint.Address,
		LatencyUs:    latency,
	}, nil
}

// toMetadataMD 将 map[string]string 转换为 metadata.MD
func toMetadataMD(m map[string]string) metadata.MD {
	md := metadata.MD{}
	for k, v := range m {
		md[k] = []string{v}
	}

	return md
}

// mdToMap 转换 metadata.MD 为普通 map
func mdToMap(md metadata.MD) map[string]string {
	m := make(map[string]string, len(md))
	for k, v := range md {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	return m
}

// Health 健康检查
func (s *BridgeServer) Health(_ context.Context, req *bridgev1.HealthRequest) (*bridgev1.HealthResponse, error) {
	return &bridgev1.HealthResponse{
		Status: bridgev1.HealthResponse_SERVING,
	}, nil
}

// Start 启动 Bridge 服务
// 1. 初始化可观测性（日志、Trace、指标）
// 2. 创建 gRPC Server，注册 BridgeService
// 3. 启动 Prometheus 指标 HTTP 服务
// 4. 监听 gRPC 端口，接收业务方请求
func Start(cfg *config.Config) error {
	// 初始化可观测性
	if err := observability.Init(cfg.Observability); err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// 创建 Bridge 服务实例
	bridge, err := New(cfg)
	if err != nil {
		return err
	}

	// 组装拦截器链（从外到内：Recovery -> Logging -> Auth -> RateLimit -> Trace）
	chain := middleware.ChainUnaryServer(
		middleware.RecoveryInterceptor,
		middleware.LoggingInterceptor,
		// middleware.AuthInterceptor,
		// middleware.RateLimitInterceptor,
	)

	// 创建 gRPC server（业务方通过此 Server 调用 Bridge）
	gs := grpc.NewServer(
		grpc.MaxConcurrentStreams(cfg.Server.MaxConcurrentStreams),
		grpc.UnaryInterceptor(chain),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     cfg.Server.KeepaliveTime,
			MaxConnectionAgeGrace: cfg.Server.KeepaliveTimeout,
		}),
	)

	bridgev1.RegisterBridgeServiceServer(gs, bridge)
	reflection.Register(gs) // 启用反射，便于 grpcurl 调试

	// 启动 metrics HTTP server（Prometheus 采集指标）
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		addr := fmt.Sprintf(":%d", cfg.Observability.MetricsPort)
		log.Info().Str("addr", addr).Msg("metrics server starting")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Error().Err(err).Msg("metrics server failed")
		}
	}()

	// 监听 gRPC 端口（业务方通过此端口调用 Bridge）
	lis, err := net.Listen("tcp", cfg.Server.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	log.Info().Str("addr", cfg.Server.ListenAddr).Msg("bridge gRPC server starting, waiting for upstream requests")
	return gs.Serve(lis)
}
