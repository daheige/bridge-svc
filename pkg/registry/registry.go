package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// DefaultTTL etcd 租约默认 TTL，单位秒。
const DefaultTTL = 10

// DefaultEtcdTimeout etcd 操作默认超时。
const DefaultEtcdTimeout = 5 * time.Second

// Endpoint 服务节点信息。
// 注册到 etcd 的数据结构，Bridge 据此进行路由和协议选择。
type Endpoint struct {
	Address  string            `json:"address"`  // 服务地址，如 "10.0.1.5:50051"
	Weight   uint32            `json:"weight"`   // 权重，用于负载均衡（默认 100）
	Protocol string            `json:"protocol"` // 协议类型：GRPC / HTTP（Bridge 根据此选择 Handler）
	Region   string            `json:"region"`   // 区域，如 "cn-north-1"
	Tags     map[string]string `json:"tags"`     // 标签，如 {"version": "v1", "env": "prod"}
	Healthy  bool              `json:"healthy"`  // 健康状态
}

// RegistryOption 服务注册器配置选项。
type RegistryOption func(*ServiceRegistry)

// WithTTL 设置 etcd 租约 TTL（秒）。
func WithTTL(ttl int64) RegistryOption {
	return func(r *ServiceRegistry) {
		if ttl > 0 {
			r.ttl = ttl
		}
	}
}

// WithEtcdTimeout 设置 etcd 操作超时。
func WithEtcdTimeout(timeout time.Duration) RegistryOption {
	return func(r *ServiceRegistry) {
		if timeout > 0 {
			r.etcdTimeout = timeout
		}
	}
}

// WithInstanceID 设置实例标识，用于构造唯一注册 key。
// 默认使用 sanitized 后的 endpoint.Address。
func WithInstanceID(id string) RegistryOption {
	return func(r *ServiceRegistry) {
		if id != "" {
			r.instanceID = sanitizeInstanceID(id)
		}
	}
}

// ServiceRegistry 服务注册器。
type ServiceRegistry struct {
	client      *clientv3.Client
	prefix      string
	service     string
	version     string
	instanceID  string
	endpoint    Endpoint
	leaseID     clientv3.LeaseID
	mu          sync.Mutex
	ttl         int64
	etcdTimeout time.Duration
	stop        chan struct{}
	once        sync.Once
}

// NewServiceRegistry 创建服务注册器。
// prefix: etcd 前缀，如 "/services/"
// service: 服务名，如 "order_service"
// version: 版本，如 "default" 或 "v2"
func NewServiceRegistry(etcdEndpoints []string, prefix, service, version string, endpoint Endpoint, opts ...RegistryOption) (*ServiceRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		prefix = "/services/"
	}
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s", prefix)

	if version == "" {
		version = "_default"
	}

	r := &ServiceRegistry{
		client:      cli,
		prefix:      prefix,
		service:     service,
		version:     version,
		instanceID:  sanitizeInstanceID(endpoint.Address),
		endpoint:    endpoint,
		ttl:         DefaultTTL,
		etcdTimeout: DefaultEtcdTimeout,
		stop:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// Register 注册服务到 etcd。
// 使用租约（lease）实现自动过期，服务异常退出时自动从 etcd 移除。
func (r *ServiceRegistry) Register() error {
	if err := r.registerOnce(); err != nil {
		return err
	}

	go r.keepAlive()
	return nil
}

func (r *ServiceRegistry) registerOnce() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	resp, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("grant lease failed: %w", err)
	}

	data, err := json.Marshal(r.endpoint)
	if err != nil {
		return fmt.Errorf("marshal endpoint failed: %w", err)
	}

	key := r.registryKey()
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("put endpoint failed: %w", err)
	}

	r.mu.Lock()
	r.leaseID = resp.ID
	r.mu.Unlock()

	log.Printf("registered service: %s leaseID:%v", key, resp.ID)
	return nil
}

// etcd key格式：/services/Hello.Greeter/v1/127.0.0.1-50051
func (r *ServiceRegistry) registryKey() string {
	return fmt.Sprintf("%s/%s/%s/%s", r.prefix, r.service, r.version, r.instanceID)
}

// keepAlive 保持租约，并在 KeepAlive channel 关闭后自动重新注册。
func (r *ServiceRegistry) keepAlive() {
	for {
		select {
		case <-r.stop:
			return
		default:
		}

		if !r.runKeepAlive() {
			return
		}

		// KeepAlive 异常退出，等待一小段时间后由外层循环重新建立。
		time.Sleep(time.Second)
	}
}

// runKeepAlive 对当前 leaseID 执行一次 KeepAlive。
// 返回 false 表示收到停止信号；返回 true 表示需要重新注册后继续。
func (r *ServiceRegistry) runKeepAlive() bool {
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()

	if leaseID == 0 {
		if err := r.reRegister(); err != nil {
			log.Printf("re-register failed (lease is zero): %v", err)
		}
		return true
	}

	ch, err := r.client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		log.Printf("keepalive init failed: %v", err)
		if err := r.reRegister(); err != nil {
			log.Printf("re-register failed: %v", err)
		}
		return true
	}

	for {
		select {
		case <-r.stop:
			return false
		case ka, ok := <-ch:
			if !ok || ka == nil {
				log.Println("keepalive channel closed, re-registering")
				if err := r.reRegister(); err != nil {
					log.Printf("re-register failed: %v", err)
				}
				return true
			}
		}
	}
}

// reRegister 在 KeepAlive 失败或 lease 失效时重新创建 lease 并写入注册信息。
func (r *ServiceRegistry) reRegister() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stop:
		return nil
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	resp, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("grant lease failed: %w", err)
	}

	data, err := json.Marshal(r.endpoint)
	if err != nil {
		return fmt.Errorf("marshal endpoint failed: %w", err)
	}

	key := r.registryKey()
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("put endpoint failed: %w", err)
	}

	r.leaseID = resp.ID
	log.Printf("re-registered service: %s leaseID:%v", key, resp.ID)
	return nil
}

// Deregister 从 etcd 注销服务，幂等且安全。
func (r *ServiceRegistry) Deregister() error {
	r.once.Do(func() {
		close(r.stop)
	})

	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	if leaseID != 0 {
		if _, err := r.client.Revoke(ctx, leaseID); err != nil {
			log.Printf("revoke lease failed: %v", err)
		}
		r.mu.Lock()
		r.leaseID = 0
		r.mu.Unlock()
	}

	key := r.registryKey()
	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("delete endpoint failed: %w", err)
	}

	log.Printf("deregistered service: %s", key)
	return nil
}

// sanitizeInstanceID 把地址等字符串转换为可用作 etcd key 的实例标识。
func sanitizeInstanceID(s string) string {
	s = strings.ReplaceAll(s, "://", "-")
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, ":", "-")
	s = strings.TrimSpace(s)
	if s == "" {
		return "default"
	}
	return s
}
