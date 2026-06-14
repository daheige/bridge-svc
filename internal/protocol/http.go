package protocol

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/daheige/bridge-svc/internal/router"
)

// HTTPHandler gRPC to HTTP 协议转换处理器
// 业务方 -> Bridge(gRPC) -> 下游 HTTP/REST 微服务
// 使用 resty/v2 作为 HTTP 客户端，支持中间件、重试、拦截器
type HTTPHandler struct {
	client *resty.Client // resty HTTP 客户端
}

// NewHTTPHandler 创建 HTTP 处理器
func NewHTTPHandler() *HTTPHandler {
	client := resty.New()
	client.SetTimeout(10 * time.Second)
	client.SetRetryCount(3)
	client.SetRetryWaitTime(100 * time.Millisecond)

	return &HTTPHandler{
		client: client,
	}
}

// Call 执行 gRPC to HTTP 转换调用
// 1. 将 gRPC 请求转换为 HTTP 请求
// 2. 透传 metadata 为 HTTP header
// 3. 发送 HTTP 请求到下游服务
// 4. 将 HTTP 响应转换为 gRPC 响应格式
func (h *HTTPHandler) Call(ctx context.Context, target *router.RouteTarget, payload *anypb.Any, md metadata.MD, timeout time.Duration) (*Response, error) {
	start := time.Now()

	// 构建请求 URL: http://host:port/service_name/method_name
	service := strings.TrimPrefix(target.ServiceName, "/")
	service = strings.TrimSuffix(service, "/")
	url := fmt.Sprintf("http://%s/%s/%s", target.Endpoint.Address, service, target.MethodName)

	// 创建带上下文的请求
	req := h.client.R().SetContext(ctx)

	// 透传 metadata 为 HTTP header（如 trace context、auth token）
	for k, v := range md {
		if len(v) > 0 {
			req.SetHeader(k, v[0])
		}
	}

	// 设置 Content-Type 和 payload
	req.SetHeader("Content-Type", "application/octet-stream")
	if payload != nil {
		req.SetBody(payload.Value)
	}

	// 执行 HTTP POST 请求
	resp, err := req.Post(url)
	latency := uint64(time.Since(start).Microseconds())

	if err != nil {
		return nil, fmt.Errorf("http call to %s: %w", target.Endpoint.Address, err)
	}

	// 将 HTTP 响应体转换为 Any（透传回业务方）
	result := &anypb.Any{
		Value: resp.Body(),
	}

	return &Response{
		Payload:   result,
		Metadata:  metadata.MD{},
		LatencyUs: latency,
	}, nil
}
