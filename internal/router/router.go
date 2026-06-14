package router

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"

	"github.com/daheige/bridge-svc/internal/config"
)

// ProtocolType 下游微服务协议类型
type ProtocolType string

const (
	ProtocolGRPC ProtocolType = "GRPC" // 下游是 gRPC 服务
	ProtocolHTTP ProtocolType = "HTTP" // 下游是 HTTP/REST 服务
)

// Endpoint 下游微服务节点
type Endpoint struct {
	Address  string            `json:"address"`  // 下游服务地址，如 "10.0.1.5:50051"
	Weight   uint32            `json:"weight"`   // 权重，用于负载均衡
	Protocol ProtocolType      `json:"protocol"` // 协议类型
	Region   string            `json:"region"`   // 区域，如 "cn-north-1"
	Tags     map[string]string `json:"tags"`     // 标签，如 {"version": "v2"}
	Healthy  bool              `json:"healthy"`  // 健康状态
}

// RouteContext 路由上下文（从业务方请求中提取）
type RouteContext struct {
	Target       string       // 目标服务路径，如 "order_service/CreateOrder"
	Protocol     ProtocolType // 协议类型
	Metadata     metadata.MD  // 元数据，透传给下游
	PreferRegion string       // 优先区域
	Canary       string       // 灰度标识
}

// RouteTarget 路由结果（选中的下游微服务节点）
type RouteTarget struct {
	ServiceName string   // 服务名，如 "order_service"，对应微服务的package.service_name
	MethodName  string   // 方法名，如 "CreateOrder"
	Version     string   // 版本号，如 "v2"
	Endpoint    Endpoint // 选中的节点
}

// Router 路由引擎
type Router struct {
	client   *clientv3.Client
	cache    *sync.Map // serviceKey -> []Endpoint
	balancer LoadBalancer
	prefix   string
}

// New 创建路由引擎，初始化 etcd 连接并启动 watch
func New(cfg *config.EtcdConfig) (*Router, error) {
	// 创建etcd实例
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd connect: %w", err)
	}

	prefix := strings.TrimPrefix(cfg.Prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s/", prefix) // 格式：/services/
	r := &Router{
		client:   cli,
		cache:    &sync.Map{},
		balancer: NewWeightedRoundRobin(),
		prefix:   prefix,
	}

	// 启动全量加载 + watch
	if err := r.bootstrap(); err != nil {
		return nil, err
	}
	go r.watch()

	return r, nil
}

// parseTarget 解析 "service/method" 或 "service/method/v2"
func parseTarget(target string) (service, method, version string) {
	parts := strings.Split(target, "/")
	switch len(parts) {
	case 2:
		return parts[0], parts[1], ""
	case 3:
		return parts[0], parts[1], parts[2]
	default:
		return target, "", ""
	}
}

// Route 执行路由决策：根据 target 找到对应的下游微服务节点
func (r *Router) Route(ctx context.Context, routeCtx RouteContext) (*RouteTarget, error) {
	service, method, version := parseTarget(routeCtx.Target)
	fmt.Printf("service: %v, version: %v, method: %v\n", service, version, method)
	key := r.serviceKey(service, version)

	fmt.Printf("key: %s", key)
	// 1. 读本地缓存
	val, ok := r.cache.Load(key)
	if !ok {
		// 2. 缓存未命中，回源 etcd
		endpoints, err := r.lookupFromEtcd(ctx, service, version)
		if err != nil {
			return nil, fmt.Errorf("lookup etcd: %w", err)
		}
		r.cache.Store(key, endpoints)
		val = endpoints
	}

	endpoints := val.([]Endpoint)
	healthy := filterHealthy(endpoints)
	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy endpoint for %s", routeCtx.Target)
	}

	// 3. 负载均衡选择节点
	selected := r.balancer.Select(healthy, routeCtx)

	return &RouteTarget{
		ServiceName: service,
		MethodName:  method,
		Version:     version,
		Endpoint:    selected,
	}, nil
}

func (r *Router) serviceKey(service, version string) string {
	if version == "" {
		return fmt.Sprintf("%s%s/%s", r.prefix, service)
	}

	return fmt.Sprintf("%s%s/%s", r.prefix, service, version)
}

func (r *Router) lookupFromEtcd(ctx context.Context, service, version string) ([]Endpoint, error) {
	key := r.serviceKey(service, version)
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var endpoints []Endpoint
	for _, kv := range resp.Kvs {
		var eps []Endpoint
		if err := json.Unmarshal(kv.Value, &eps); err != nil {
			log.Warn().Err(err).Str("key", string(kv.Key)).Msg("unmarshal endpoint failed")
			continue
		}
		endpoints = append(endpoints, eps...)
	}

	return endpoints, nil
}

// watch 持续监听 etcd 变更，增量更新本地缓存
func (r *Router) watch() {
	watchChan := r.client.Watch(context.Background(), r.prefix, clientv3.WithPrefix())
	for wresp := range watchChan {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				var eps []Endpoint
				if err := json.Unmarshal(ev.Kv.Value, &eps); err != nil {
					continue
				}
				r.cache.Store(string(ev.Kv.Key), eps)
			case clientv3.EventTypeDelete:
				r.cache.Delete(string(ev.Kv.Key))
			}
		}
	}
}

// bootstrap 启动时全量加载 etcd 数据到本地缓存
func (r *Router) bootstrap() error {
	resp, err := r.client.Get(context.Background(), r.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		var eps []Endpoint
		if err := json.Unmarshal(kv.Value, &eps); err != nil {
			continue
		}
		r.cache.Store(string(kv.Key), eps)
	}
	return nil
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
