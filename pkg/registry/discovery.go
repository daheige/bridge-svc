package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

// Discovery 服务发现接口。
type Discovery interface {
	// Get 返回指定服务和版本的所有可用节点。
	Get(ctx context.Context, service, version string) ([]Endpoint, error)
	// Watch 监听指定服务和版本的节点变化。
	Watch(ctx context.Context, service, version string) (Watcher, error)
}

// Watcher 服务发现监听器。
type Watcher interface {
	// Next 阻塞等待下一次节点列表变化，返回最新全量节点列表。
	Next() ([]Endpoint, error)
	// Stop 停止监听。
	Stop()
}

// etcdDiscovery 基于 etcd 的服务发现实现。
type etcdDiscovery struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
}

// NewEtcdDiscovery 创建 etcd 服务发现器。
// prefix 与注册端保持一致，如 "/services"。
func NewEtcdDiscovery(etcdEndpoints []string, prefix string, timeout time.Duration) (Discovery, error) {
	if timeout <= 0 {
		timeout = DefaultEtcdTimeout
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		prefix = "/services"
	}
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s", prefix)

	return &etcdDiscovery{
		client:  cli,
		prefix:  prefix,
		timeout: timeout,
	}, nil
}

func (d *etcdDiscovery) Get(ctx context.Context, service, version string) ([]Endpoint, error) {
	prefix := d.servicePrefix(service, version)

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	resp, err := d.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}

	return parseEndpoints(resp.Kvs), nil
}

func (d *etcdDiscovery) Watch(ctx context.Context, service, version string) (Watcher, error) {
	prefix := d.servicePrefix(service, version)

	w := &etcdWatcher{
		ch:     make(chan []Endpoint, 1),
		stopCh: make(chan struct{}),
	}

	// 先获取一次全量作为初始值。
	endpoints, err := d.Get(ctx, service, version)
	if err != nil {
		return nil, err
	}
	w.ch <- endpoints

	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go func() {
		defer close(w.ch)
		watchCh := d.client.Watch(watchCtx, prefix, clientv3.WithPrefix())
		for {
			select {
			case <-w.stopCh:
				return
			case resp, ok := <-watchCh:
				if !ok {
					return
				}
				if resp.Err() != nil {
					// 出错时忽略本次事件，等待下一次 watch 事件或重新初始化。
					continue
				}

				// 每次收到事件后重新拉取全量，保证删除/更新都能正确反映。
				endpoints, err := d.Get(watchCtx, service, version)
				if err != nil {
					continue
				}
				if !w.push(endpoints) {
					return
				}
			}
		}
	}()

	return w, nil
}

func (d *etcdDiscovery) servicePrefix(service, version string) string {
	if version == "" {
		version = "_default"
	}
	return fmt.Sprintf("%s/%s/%s/", d.prefix, service, version)
}

// etcdWatcher etcd 监听器实现。
type etcdWatcher struct {
	ch     chan []Endpoint
	stopCh chan struct{}
	cancel context.CancelFunc
	once   sync.Once
}

func (w *etcdWatcher) Next() ([]Endpoint, error) {
	endpoints, ok := <-w.ch
	if !ok {
		return nil, fmt.Errorf("watcher closed")
	}
	return endpoints, nil
}

func (w *etcdWatcher) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
		if w.cancel != nil {
			w.cancel()
		}
	})
}

func (w *etcdWatcher) push(endpoints []Endpoint) bool {
	select {
	case w.ch <- endpoints:
		return true
	case <-w.stopCh:
		return false
	}
}

func parseEndpoints(kvs []*mvccpb.KeyValue) []Endpoint {
	seen := make(map[string]struct{})
	var endpoints []Endpoint
	for _, kv := range kvs {
		var ep Endpoint
		if err := json.Unmarshal(kv.Value, &ep); err != nil {
			continue
		}
		if ep.Address == "" {
			continue
		}
		if _, ok := seen[ep.Address]; ok {
			continue
		}
		seen[ep.Address] = struct{}{}
		endpoints = append(endpoints, ep)
	}
	return endpoints
}
