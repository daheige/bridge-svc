package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Client 管理一组下游 gRPC 服务连接。
type Client struct {
	cfg      *Config
	services map[string]*Service
	opts     ClientOptions
	mu       sync.RWMutex
}

// NewClient 根据 services 配置创建下游 gRPC 连接。
func NewClient(cfg *Config, opts ...ClientOption) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	o := ClientOptions{
		DefaultTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}

	client := &Client{
		cfg:      cfg,
		services: make(map[string]*Service, len(cfg.Services)),
		opts:     o,
	}

	for i := range cfg.Services {
		svcCfg := &cfg.Services[i]
		if svcCfg.Name == "" {
			return nil, fmt.Errorf("service name is required at index %d", i)
		}
		if svcCfg.Target == "" {
			return nil, fmt.Errorf("service %s target is required", svcCfg.Name)
		}

		svc, err := newService(svcCfg, o)
		if err != nil {
			return nil, fmt.Errorf("create service %s: %w", svcCfg.Name, err)
		}
		client.services[svcCfg.Name] = svc
	}

	return client, nil
}

// Service 按逻辑服务名返回对应的服务调用包装器。
func (c *Client) Service(name string) (*Service, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	svc, ok := c.services[name]
	if !ok {
		return nil, serviceNotFound(name)
	}

	return svc, nil
}

// Invoke 按服务名调用下游 gRPC 方法。
// method 为方法名，如 SayHello；req/resp 为请求与响应 protobuf 消息。
func (c *Client) Invoke(ctx context.Context, serviceName, method string, req, resp proto.Message, opts ...grpc.CallOption) error {
	svc, err := c.Service(serviceName)
	if err != nil {
		return err
	}

	return svc.Invoke(ctx, method, req, resp, opts...)
}

// InvokeFull 按完整 gRPC 方法路径调用，如 "/Hello.Greeter/SayHello"。
func (c *Client) InvokeFull(ctx context.Context, serviceName, fullMethod string, req, resp proto.Message, opts ...grpc.CallOption) error {
	svc, err := c.Service(serviceName)
	if err != nil {
		return err
	}

	return svc.InvokeFull(ctx, fullMethod, req, resp, opts...)
}

// Close 关闭所有下游服务连接。
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, svc := range c.services {
		if err := svc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// mergeMetadata 合并服务默认 metadata 与上下文中已有的 metadata。
func mergeMetadata(ctx context.Context, md map[string]string) context.Context {
	if len(md) == 0 {
		return ctx
	}

	out := make(metadata.MD, len(md))
	for k, v := range md {
		out.Set(k, v)
	}

	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		for k, vv := range existing {
			if _, has := out[k]; !has {
				out[k] = vv
			}
		}
	}

	return metadata.NewOutgoingContext(ctx, out)
}

// withTimeout 如果 ctx 没有设置 deadline，则根据服务或默认超时自动设置。
func withTimeout(ctx context.Context, svcTimeout, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	timeout := svcTimeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	if _, ok := ctx.Deadline(); !ok && timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return ctx, func() {}
}
