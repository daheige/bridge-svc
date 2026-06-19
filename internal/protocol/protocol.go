package protocol

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/daheige/registry"

	"github.com/daheige/bridge-svc/internal/router"
)

// Handler 协议处理器接口
// Bridge 根据下游微服务的协议类型，选择对应的 Handler 进行转发
type Handler interface {
	// Call 执行协议转发
	// ctx: 上下文
	// target: 路由目标（下游微服务节点信息）
	// payload: 业务负载（从业务方请求中透传）
	// md: 元数据（从业务方请求中透传）
	// timeout: 超时时间
	Call(ctx context.Context, target *router.RouteTarget, payload *anypb.Any, md metadata.MD, timeout time.Duration) (*Response, error)
}

// Response 统一响应结构（从下游微服务返回）
type Response struct {
	Payload   *anypb.Any  // 响应负载
	Metadata  metadata.MD // 响应元数据
	LatencyUs uint64      // 调用耗时（微秒）
}

// Factory 创建对应协议的处理器
// 根据下游微服务的协议类型，创建对应的 Handler
func Factory(protocol registry.ProtocolType) Handler {
	switch protocol {
	case registry.ProtocolGRPC:
		return NewGRPCHandler() // 下游是 gRPC 服务
	case registry.ProtocolHTTP:
		return NewHTTPHandler() // 下游是 HTTP 服务
	default:
		return nil
	}
}
