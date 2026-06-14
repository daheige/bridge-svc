package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/daheige/hello-pb/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"

	bridgev1 "github.com/daheige/bridge-svc/api/v1"
)

func main() {
	// 1. 建立到 Bridge 的 gRPC 连接
	address := "localhost:50052"
	conn, err := grpc.NewClient(
		address,
		// 如果使用k8s命名服务以及headless方式访问，需要打开下面的注释，实现客户端负载均衡
		// 关键配置：启用round_robin负载均衡策略
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(30*time.Minute), // 连接生命周期
		grpc.WithMaxCallAttempts(3),          // 最大重试次数
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// 2. 创建 Bridge 客户端
	client := bridgev1.NewBridgeServiceClient(conn)

	// 3. 构建业务请求（以调用订单服务创建订单为例）
	req := &pb.HelloReq{
		Name: "daheige",
	}

	// 4. 将业务请求打包为 Any（Bridge 不感知具体 Schema）
	payload, err := anypb.New(req)
	if err != nil {
		log.Fatal(err)
	}

	// 5. 调用 Bridge
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
		// target: 下游服务名/方法名，对应 gRPC 方法路径，如 /Hello.Greeter/SayHello，对应：包名.服务名/方法名称
		Target: "Hello.Greeter/SayHello/v1",
		// protocol: 下游服务协议类型（GRPC/HTTP）
		Protocol: "GRPC",
		// payload: 业务请求负载
		Payload: payload,
		// metadata: 透传给下游的元数据（如认证、追踪等）
		Metadata: map[string]string{
			"authorization": "Bearer token123",
			"x-request-id":  "req-456",
			"x-trace-id":    "trace-789",
		},
		// timeout_ms: Bridge 到下游的超时（毫秒）
		TimeoutMs: 3000,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 6. 处理响应
	if resp.Status != nil && resp.Status.Code != 0 {
		fmt.Printf("Error: %s", resp.Status.Message)
		return
	}

	// 7. 从 Any 解包业务响应
	var res pb.HelloReply
	if err := resp.Payload.UnmarshalTo(&res); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("res message:%s", res.Message)
}
