package protocol

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/daheige/bridge-svc/internal/pool"
	"github.com/daheige/bridge-svc/internal/router"
)

// GRPCHandler gRPC 协议透传处理器
type GRPCHandler struct {
	pool *pool.GRPCPool
}

func NewGRPCHandler() *GRPCHandler {
	return &GRPCHandler{
		pool: pool.NewGRPCPool(),
	}
}

// Call 执行 gRPC 透传调用
func (h *GRPCHandler) Call(ctx context.Context, target *router.RouteTarget, payload *anypb.Any, md metadata.MD, timeout time.Duration) (*Response, error) {
	start := time.Now()

	// 1. 从连接池获取连接
	conn, err := h.pool.Get(ctx, target.Endpoint.Address)
	if err != nil {
		return nil, fmt.Errorf("get connection to downstream %s: %w", target.Endpoint.Address, err)
	}
	defer h.pool.Put(conn)

	// 2. 构建 gRPC 方法路径
	service := strings.TrimPrefix(target.ServiceName, "/")
	service = strings.TrimSuffix(service, "/")
	method := fmt.Sprintf("/%s/%s", service, target.MethodName)

	log.Println("call method:", method)

	// 3. 透传 metadata
	ctx = metadata.NewOutgoingContext(ctx, md)

	// 4. 设置超时
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 5. 使用 rawBytesCodec 透传，避免 Bridge 反序列化业务消息
	// 请求：提取 Any 的原始字节
	reqBytes := payload.GetValue()

	// 响应：用 rawBytes 接收原始字节，再包装回 Any
	var respBytes []byte
	err = conn.Invoke(ctx, method, reqBytes, &respBytes, grpc.ForceCodec(&rawBytesCodec{}))

	latency := uint64(time.Since(start).Microseconds())

	if err != nil {
		return nil, fmt.Errorf("invoke downstream %s: %w", method, err)
	}

	// 6. 将原始字节包装回 anypb.Any，TypeUrl 保持与请求一致或留空（由业务方解析）
	// 注意：这里 TypeUrl 无法从下游推断，通常 Bridge 透传场景下业务方知道期望类型
	// 如果下游返回的 trailer 中有类型信息，可以从 trailer 提取
	respAny := &anypb.Any{
		Value: respBytes,
		// TypeUrl 不设置，业务方根据路由信息自行解析
	}

	return &Response{
		Payload:   respAny,
		Metadata:  metadata.MD{}, // 可从 response trailer 提取
		LatencyUs: latency,
	}, nil
}

// rawBytes 是原始字节的别名，用于 codec 透传
type rawBytes []byte

// rawBytesCodec 实现 grpc.Codec，直接透传原始字节流
// 避免 Bridge 对业务消息进行 protobuf 序列化/反序列化
type rawBytesCodec struct{}

func (c *rawBytesCodec) Marshal(v interface{}) ([]byte, error) {
	switch msg := v.(type) {
	case rawBytes:
		return msg, nil
	case []byte:
		return msg, nil
	case *anypb.Any:
		return msg.Value, nil
	case proto.Message:
		return proto.Marshal(msg)
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

func (c *rawBytesCodec) Unmarshal(data []byte, v interface{}) error {
	switch msg := v.(type) {
	case *rawBytes:
		*msg = data
		return nil
	case *[]byte:
		*msg = data
		return nil
	case *anypb.Any:
		// 关键修正：只填充 Value，不碰 TypeUrl
		// 避免 "mismatched message type" 错误
		msg.Value = data
		return nil
	case proto.Message:
		return proto.Unmarshal(data, msg)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}

func (c *rawBytesCodec) Name() string {
	return "raw-bytes"
}
