package protocol

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/daheige/bridge-svc/internal/pool"
	"github.com/daheige/bridge-svc/internal/router"
)

// GRPCHandler gRPC 协议透传处理器
// 业务方 -> Bridge(gRPC) -> 下游 gRPC 微服务
// 此 Handler 负责将 Bridge 接收到的 gRPC 请求转发到下游 gRPC 服务
type GRPCHandler struct {
	pool      *pool.GRPCPool
	typeCache *methodTypeCache
}

// NewGRPCHandler 创建 gRPC 处理器
func NewGRPCHandler() *GRPCHandler {
	return &GRPCHandler{
		pool:      pool.NewGRPCPool(),
		typeCache: globalMethodTypeCache,
	}
}

// Call 执行 gRPC 透传调用
// 1. 从连接池获取到下游 gRPC 服务的连接
// 2. 构建 gRPC 方法路径（/PackageName.ServiceName/MethodName）
// 3. 透传 metadata（trace context、auth token 等）
// 4. 使用 rawBytesCodec 透传 payload，Bridge 不反序列化业务消息
// 5. 通过 gRPC reflection 获取下游方法输出类型，填充响应 anypb.Any 的 TypeUrl
// 6. 调用下游服务并返回响应
func (h *GRPCHandler) Call(ctx context.Context, target *router.RouteTarget, payload *anypb.Any, md metadata.MD, timeout time.Duration) (*Response, error) {
	start := time.Now()

	// 1. 从连接池获取或创建到下游 gRPC 服务的连接
	conn, err := h.pool.Get(ctx, target.Endpoint.Address)
	if err != nil {
		return nil, fmt.Errorf("get connection to downstream %s: %w", target.Endpoint.Address, err)
	}
	defer h.pool.Put(conn)

	// 2. 构建 gRPC 方法路径（full method name）
	// target.ServiceName 已经是 "PackageName.ServiceName" 格式
	// target.MethodName 是 "MethodName"
	service := strings.TrimPrefix(target.ServiceName, "/")
	service = strings.TrimSuffix(service, "/")
	method := fmt.Sprintf("/%s/%s", service, target.MethodName)

	log.Println("call method:", method)

	// 3. 透传 metadata 到下游（包含 trace context、auth token 等）
	ctx = metadata.NewOutgoingContext(ctx, md)

	// 4. 设置超时
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 5. 使用 rawBytesCodec 透传业务消息
	// Bridge 不感知具体的业务类型，直接透传 Any 的 Value 字节到下游
	// 下游服务会根据自己的 proto 定义解析这些字节
	reqBytes := payload.GetValue()

	var respBytes []byte
	err = conn.Invoke(ctx, method, reqBytes, &respBytes, grpc.ForceCodec(&rawBytesCodec{}))

	latency := uint64(time.Since(start).Microseconds())

	if err != nil {
		return nil, fmt.Errorf("invoke downstream %s: %w", method, err)
	}

	// 6. 将原始字节包装回 anypb.Any，并通过 gRPC reflection 获取输出类型填充 TypeUrl
	// TypeUrl 用于客户端调用 anypb.Any.UnmarshalTo 时进行类型校验
	respAny := &anypb.Any{
		Value: respBytes,
	}
	// 使用独立的短超时上下文进行 reflection，避免影响主调用耗时
	reflectCtx, reflectCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer reflectCancel()
	outputType, typeErr := h.typeCache.getOutputType(reflectCtx, conn, target.Endpoint.Address, method)
	if typeErr != nil {
		log.Printf("warn: failed to get output type via reflection for %s: %v", method, typeErr)
	} else if outputType != "" {
		respAny.TypeUrl = "type.googleapis.com/" + outputType
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

// globalMethodTypeCache 全局方法输出类型缓存
// 用于避免每次请求都进行 gRPC reflection 查询
var globalMethodTypeCache = newMethodTypeCache()

// methodTypeCache 缓存下游 gRPC 方法的输出类型全名
// key: addr + method, value: proto 全名（如 Hello.HelloReply）
type methodTypeCache struct {
	mu sync.RWMutex
	m  map[string]string
}

func newMethodTypeCache() *methodTypeCache {
	return &methodTypeCache{m: make(map[string]string)}
}

func (c *methodTypeCache) getOutputType(ctx context.Context, conn *grpc.ClientConn, addr, method string) (string, error) {
	key := addr + method

	c.mu.RLock()
	if t, ok := c.m[key]; ok {
		c.mu.RUnlock()
		return t, nil
	}
	c.mu.RUnlock()

	t, err := queryOutputType(ctx, conn, method)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.m[key] = t
	c.mu.Unlock()
	return t, nil
}

// queryOutputType 通过 gRPC reflection 查询指定方法的输出消息类型全名
// method 格式: /PackageName.ServiceName/MethodName，如 /Hello.Greeter/SayHello
func queryOutputType(ctx context.Context, conn *grpc.ClientConn, method string) (string, error) {
	parts := strings.Split(method, "/")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("invalid method format: %s", method)
	}
	serviceName := parts[1]
	methodName := parts[2]

	client := grpc_reflection_v1alpha.NewServerReflectionClient(conn)
	stream, err := client.ServerReflectionInfo(ctx)
	if err != nil {
		return "", fmt.Errorf("create reflection stream: %w", err)
	}
	defer stream.CloseSend()

	req := &grpc_reflection_v1alpha.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: serviceName,
		},
	}
	if err := stream.Send(req); err != nil {
		return "", fmt.Errorf("send reflection request: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("recv reflection response: %w", err)
	}

	fds := resp.GetFileDescriptorResponse().GetFileDescriptorProto()
	if len(fds) == 0 {
		return "", fmt.Errorf("empty file descriptor response")
	}

	var fdp descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(fds[0], &fdp); err != nil {
		return "", fmt.Errorf("unmarshal file descriptor: %w", err)
	}

	// 服务短名（如 Hello.Greeter -> Greeter）
	shortName := serviceName
	if idx := strings.LastIndex(serviceName, "."); idx >= 0 {
		shortName = serviceName[idx+1:]
	}

	for _, svc := range fdp.Service {
		if svc.GetName() != shortName {
			continue
		}
		for _, m := range svc.Method {
			if m.GetName() == methodName {
				return strings.TrimPrefix(m.GetOutputType(), "."), nil
			}
		}
	}

	return "", fmt.Errorf("method %s/%s not found in descriptor", serviceName, methodName)
}
