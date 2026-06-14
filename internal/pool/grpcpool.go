package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCPool gRPC 连接池
// 管理 Bridge 到下游 gRPC 微服务的连接，实现连接复用
type GRPCPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn // key: 下游地址, value: gRPC 连接
}

// NewGRPCPool 创建连接池
func NewGRPCPool() *GRPCPool {
	return &GRPCPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Get 获取或创建到下游 gRPC 服务的连接
func (p *GRPCPool) Get(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	// 清理地址中的 http:// 或 https:// 前缀（gRPC 不需要）
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()

	if ok && p.isHealthy(conn) {
		return conn, nil
	}

	// 创建新连接
	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if conn, ok := p.conns[addr]; ok && p.isHealthy(conn) {
		return conn, nil
	}

	newConn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16<<20), // 16MB
			grpc.MaxCallSendMsgSize(16<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	p.conns[addr] = newConn
	return newConn, nil
}

// Put 归还连接（gRPC 连接长期复用，无需真正归还）
func (p *GRPCPool) Put(conn *grpc.ClientConn) {
	// gRPC 连接长期复用，这里仅做健康检查占位
}

func (p *GRPCPool) isHealthy(conn *grpc.ClientConn) bool {
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close 关闭所有连接
func (p *GRPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		conn.Close()
	}
}
