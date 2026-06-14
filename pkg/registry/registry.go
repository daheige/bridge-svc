package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// Endpoint 服务节点信息
// 注册到 etcd 的数据结构，Bridge 据此进行路由和协议选择
type Endpoint struct {
	Address  string            `json:"address"`  // 服务地址，如 "10.0.1.5:50051"
	Weight   uint32            `json:"weight"`   // 权重，用于负载均衡（默认 100）
	Protocol string            `json:"protocol"` // 协议类型：GRPC / HTTP（Bridge 根据此选择 Handler）
	Region   string            `json:"region"`   // 区域，如 "cn-north-1"
	Tags     map[string]string `json:"tags"`     // 标签，如 {"version": "v1", "env": "prod"}
	Healthy  bool              `json:"healthy"`  // 健康状态
}

// ServiceRegistry 服务注册器
type ServiceRegistry struct {
	client   *clientv3.Client
	prefix   string
	leaseID  clientv3.LeaseID
	service  string
	version  string
	endpoint Endpoint
	stop     chan struct{}
}

// NewServiceRegistry 创建服务注册器
// prefix: etcd 前缀，如 "/services/"
// service: 服务名，如 "order_service"
// version: 版本，如 "default" 或 "v2"
func NewServiceRegistry(etcdEndpoints []string, prefix, service, version string, endpoint Endpoint) (*ServiceRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if version == "" {
		version = "default"
	}
	if prefix == "" {
		prefix = "/services/"
	}

	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s/", prefix)
	return &ServiceRegistry{
		client:   cli,
		prefix:   prefix,
		service:  service,
		version:  version,
		endpoint: endpoint,
		stop:     make(chan struct{}, 1),
	}, nil
}

// Register 注册服务到 etcd
// 使用租约（lease）实现自动过期，服务异常退出时自动从 etcd 移除
func (r *ServiceRegistry) Register() error {
	// 创建租约，TTL 为 10 秒，需要定期续租
	resp, err := r.client.Grant(context.Background(), 10)
	if err != nil {
		return err
	}
	r.leaseID = resp.ID

	// 构建注册数据
	endpoints := []Endpoint{r.endpoint}
	data, err := json.Marshal(endpoints)
	if err != nil {
		return err
	}

	// 注册到 etcd: /services/{service}/{version}
	key := fmt.Sprintf("%s/%s/%s", r.prefix, r.service, r.version)
	_, err = r.client.Put(context.Background(), key, string(data), clientv3.WithLease(r.leaseID))
	if err != nil {
		return err
	}

	// 启动续租协程
	go r.keepAlive()

	return nil
}

// keepAlive 定期续租，保持服务注册状态
func (r *ServiceRegistry) keepAlive() {
	ch, err := r.client.KeepAlive(context.Background(), r.leaseID)
	if err != nil {
		return
	}

	for range ch {
		select {
		case <-r.stop:
			log.Printf("stop keepalive leaseID:%v for etcd registry", r.leaseID)
			return
		default:
			// 续租成功，无需处理
			// log.Printf("keepalive leaseID:%v for etcd registry\n", r.leaseID)
		}
	}
}

// Deregister 从 etcd 注销服务
func (r *ServiceRegistry) Deregister() error {
	ctx := context.Background()
	key := fmt.Sprintf("%s/%s/%s", r.prefix, r.service, r.version)
	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return err
	}

	close(r.stop)
	return nil
}
