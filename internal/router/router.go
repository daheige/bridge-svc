package router

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/daheige/registry"
)

// ProtocolType 下游微服务协议类型（复用 registry 定义）。
type ProtocolType = registry.ProtocolType

const (
	ProtocolGRPC = registry.ProtocolGRPC
	ProtocolHTTP = registry.ProtocolHTTP
)

// Endpoint 下游微服务节点（复用 registry 定义）。
type Endpoint = registry.Endpoint

// RouteContext 路由上下文（从业务方请求中提取）。
type RouteContext struct {
	Target       string       // 目标服务路径，如 "order_service/CreateOrder"
	Version      string       // 目标版本，如 "v1"；空表示无版本
	Protocol     ProtocolType // 协议类型
	Metadata     metadata.MD  // 元数据，透传给下游
	PreferRegion string       // 优先区域
	Canary       string       // 灰度标识
}

// RouteTarget 路由结果（选中的下游微服务节点）。
type RouteTarget struct {
	ServiceName string   // 服务名，如 "order_service"
	MethodName  string   // 方法名，如 "CreateOrder"
	Version     string   // 版本号，如 "v2"
	Endpoint    Endpoint // 选中的节点
}

// Router 路由引擎，只依赖 registry.Discovery 接口，不依赖具体实现。
type Router struct {
	discovery registry.Discovery
	balancer  LoadBalancer
}

// New 创建路由引擎。
func New(discovery registry.Discovery) (*Router, error) {
	if discovery == nil {
		return nil, fmt.Errorf("discovery is nil")
	}

	return &Router{
		discovery: discovery,
		balancer:  NewWeightedRoundRobin(),
	}, nil
}

// parseTarget 解析 "service/method" 格式。
// 版本信息通过独立的 RouteContext.Version 传递，不再从 target 中解析。
func parseTarget(target string) (service, method string) {
	parts := strings.Split(target, "/")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return target, ""
}

// Route 执行路由决策：根据 target 和 version 找到对应的下游微服务节点。
func (r *Router) Route(ctx context.Context, routeCtx RouteContext) (*RouteTarget, error) {
	service, method := parseTarget(routeCtx.Target)
	version := routeCtx.Version

	endpoints, err := r.discovery.Get(ctx, service, version)
	if err != nil {
		return nil, fmt.Errorf("lookup endpoints: %w", err)
	}

	healthy := filterHealthy(endpoints)
	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy endpoint for %s", routeCtx.Target)
	}

	selected := r.balancer.Select(healthy, routeCtx)

	return &RouteTarget{
		ServiceName: service,
		MethodName:  method,
		Version:     version,
		Endpoint:    selected,
	}, nil
}

func filterHealthy(endpoints []Endpoint) []Endpoint {
	var result []Endpoint
	for _, ep := range endpoints {
		if ep.Healthy {
			result = append(result, ep)
		}
	}

	return result
}
