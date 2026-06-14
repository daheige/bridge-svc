package registry

import (
	"context"
	"log"
	"strings"
	"sync"

	"google.golang.org/grpc/resolver"
)

const defaultScheme = "etcd"

// etcdResolverBuilder 基于 Discovery 的 gRPC resolver 构造器。
type etcdResolverBuilder struct {
	discovery Discovery
	scheme    string
}

// NewEtcdResolverBuilder 创建 gRPC resolver builder。
// discovery 用于服务发现；scheme 为命名方案，传空则使用 "etcd"。
func NewEtcdResolverBuilder(discovery Discovery, scheme string) resolver.Builder {
	if scheme == "" {
		scheme = defaultScheme
	}
	return &etcdResolverBuilder{
		discovery: discovery,
		scheme:    scheme,
	}
}

// Build 实现 resolver.Builder。
func (b *etcdResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	service, version := parseTarget(target)

	ctx, cancel := context.WithCancel(context.Background())
	r := &etcdResolver{
		cc:        cc,
		cancel:    cancel,
		stopCh:    make(chan struct{}),
		service:   service,
		version:   version,
		discovery: b.discovery,
	}

	// 初始化地址列表。
	endpoints, err := b.discovery.Get(ctx, service, version)
	if err != nil {
		cancel()
		return nil, err
	}
	r.updateState(endpoints)

	// 启动 watch。
	watcher, err := b.discovery.Watch(ctx, service, version)
	if err != nil {
		cancel()
		return nil, err
	}
	r.watcher = watcher

	go r.watch()

	return r, nil
}

// Scheme 实现 resolver.Builder。
func (b *etcdResolverBuilder) Scheme() string {
	return b.scheme
}

// etcdResolver gRPC resolver 实现。
type etcdResolver struct {
	cc        resolver.ClientConn
	cancel    context.CancelFunc
	stopCh    chan struct{}
	once      sync.Once
	service   string
	version   string
	discovery Discovery
	watcher   Watcher
}

// ResolveNow 实现 resolver.Resolver。
func (r *etcdResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	endpoints, err := r.discovery.Get(context.Background(), r.service, r.version)
	if err != nil {
		log.Printf("etcd resolver resolve now failed: %v", err)
		return
	}
	r.updateState(endpoints)
}

// Close 实现 resolver.Resolver。
func (r *etcdResolver) Close() {
	r.once.Do(func() {
		close(r.stopCh)
		r.cancel()
		if r.watcher != nil {
			r.watcher.Stop()
		}
	})
}

func (r *etcdResolver) watch() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		endpoints, err := r.watcher.Next()
		if err != nil {
			log.Printf("etcd resolver watch closed: %v", err)
			return
		}
		r.updateState(endpoints)
	}
}

func (r *etcdResolver) updateState(endpoints []Endpoint) {
	addrs := make([]resolver.Address, 0, len(endpoints))
	for _, ep := range endpoints {
		if ep.Address == "" {
			continue
		}
		addrs = append(addrs, resolver.Address{Addr: ep.Address})
	}

	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		log.Printf("update resolver state failed: %v", err)
	}
}

func parseTarget(target resolver.Target) (service, version string) {
	path := target.URL.Path
	if path == "" {
		path = target.Endpoint()
	}
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)
	service = parts[0]
	if len(parts) > 1 {
		version = parts[1]
	}
	return
}

// RegisterEtcdResolver 使用指定 Discovery 注册 etcd gRPC resolver。
func RegisterEtcdResolver(discovery Discovery, scheme string) {
	resolver.Register(NewEtcdResolverBuilder(discovery, scheme))
}
