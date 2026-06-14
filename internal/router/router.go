package router

import (
	"context"
	"encoding/json"
	"errors"
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
	prefix = fmt.Sprintf("/%s", prefix) // 格式：/services
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
	// fmt.Printf("service: %v, version: %v, method: %v\n", service, version, method)
	key := r.serviceKey(service, version)
	// key: /services/Hello.Greeter/v1
	// fmt.Printf("key: %s", key)

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

	endpoints, ok := val.([]Endpoint)
	if !ok {
		return nil, errors.New("not found endpoints")
	}

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
		version = "_default"
	}

	return fmt.Sprintf("%s/%s/%s", r.prefix, service, version)
}

// parseEndpoint 解析 etcd value 中的单个 Endpoint，只保留健康节点。
// registry.Register 写入的是单个 Endpoint 的 JSON，不是数组。
func parseEndpoint(key string, value []byte) (Endpoint, bool) {
	var ep Endpoint
	if err := json.Unmarshal(value, &ep); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("unmarshal endpoint failed")
		return Endpoint{}, false
	}

	if !ep.Healthy {
		return Endpoint{}, false
	}

	return ep, true
}

// serviceVersionPrefix 返回某服务版本下的所有实例前缀：/services/{service}/{version}/
func (r *Router) serviceVersionPrefix(service, version string) string {
	if version == "" {
		version = "_default"
	}

	return fmt.Sprintf("%s/%s/%s/", r.prefix, service, version)
}

// serviceVersionFromKey 从完整注册键 /services/{service}/{version}/{instanceID}
// 中提取服务版本前缀 /services/{service}/{version}
func (r *Router) serviceVersionFromKey(key string) string {
	key = strings.TrimPrefix(key, r.prefix)
	key = strings.Trim(key, "/")
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return ""
	}

	service := parts[0]
	version := parts[1]
	if version == "_default" {
		version = ""
	}

	return r.serviceKey(service, version)
}

// 从etcd获取端点连接信息，只返回健康节点
func (r *Router) lookupFromEtcd(ctx context.Context, service, version string) ([]Endpoint, error) {
	prefix := r.serviceVersionPrefix(service, version)
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var endpoints []Endpoint
	for _, kv := range resp.Kvs {
		if ep, ok := parseEndpoint(string(kv.Key), kv.Value); ok {
			endpoints = append(endpoints, ep)
		}
	}

	return endpoints, nil
}

// refreshServiceCache 重新聚合某个服务版本下的所有健康实例并写入缓存。
func (r *Router) refreshServiceCache(serviceVersionKey string) error {
	parts := strings.Split(strings.Trim(serviceVersionKey, "/"), "/")
	if len(parts) < 2 {
		return nil
	}

	service := parts[len(parts)-2]
	version := parts[len(parts)-1]
	if service == "" {
		return nil
	}

	endpoints, err := r.lookupFromEtcd(context.Background(), service, version)
	if err != nil {
		return err
	}

	if len(endpoints) > 0 {
		r.cache.Store(serviceVersionKey, endpoints)
	} else {
		r.cache.Delete(serviceVersionKey)
	}

	return nil
}

// watch 持续监听 etcd 变更，只缓存健康节点；若某服务全部下线则剔除缓存
func (r *Router) watch() {
	watchChan := r.client.Watch(context.Background(), r.prefix, clientv3.WithPrefix())
	for resp := range watchChan {
		for _, ev := range resp.Events {
			serviceVersionKey := r.serviceVersionFromKey(string(ev.Kv.Key))
			if serviceVersionKey == "" {
				continue
			}

			switch ev.Type {
			case clientv3.EventTypePut:
				if _, ok := parseEndpoint(string(ev.Kv.Key), ev.Kv.Value); !ok {
					r.cache.Delete(serviceVersionKey)
					continue
				}

				if err := r.refreshServiceCache(serviceVersionKey); err != nil {
					log.Warn().Err(err).Str("key", serviceVersionKey).Msg("refresh service cache failed")
				}
			case clientv3.EventTypeDelete:
				if err := r.refreshServiceCache(serviceVersionKey); err != nil {
					log.Warn().Err(err).Str("key", serviceVersionKey).Msg("refresh service cache failed")
				}
			}
		}
	}
}

// bootstrap 启动时全量加载 etcd 数据到本地缓存，按服务版本聚合，只保留健康节点
func (r *Router) bootstrap() error {
	resp, err := r.client.Get(context.Background(), r.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	groups := make(map[string][]Endpoint)
	for _, kv := range resp.Kvs {
		ep, ok := parseEndpoint(string(kv.Key), kv.Value)
		if !ok {
			continue
		}

		serviceVersionKey := r.serviceVersionFromKey(string(kv.Key))
		if serviceVersionKey == "" {
			continue
		}

		groups[serviceVersionKey] = append(groups[serviceVersionKey], ep)
	}

	for key, eps := range groups {
		r.cache.Store(key, eps)
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
