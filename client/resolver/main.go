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

	"github.com/daheige/hephfx/hestia/etcd"

	bridgev1 "github.com/daheige/bridge-svc/api/v1"
)

func main() {
	// 1. 创建 etcd 服务发现器
	// 实际使用时应从配置文件读取 etcd 地址与前缀。
	discovery, err := etcd.NewDiscovery(
		[]string{"localhost:12379"}, // etcd 集群地址
		etcd.WithDialTimeout(10*time.Second),
		etcd.WithPrefix("services"),
	)
	if err != nil {
		log.Fatalf("create etcd discovery failed: %v", err)
	}

	// 2. 注册 etcd gRPC resolver，scheme 使用 "etcd"
	etcd.RegisterEtcdResolver(discovery)

	// 3. 通过 etcd resolver 发现 Bridge 服务并建立 gRPC 连接
	// target 格式：etcd:///<service>/<version>
	// 此处假设 Bridge 服务以 service="bridge-svc"、空版本注册到 etcd。
	conn, err := grpc.NewClient(
		"etcd:///bridge-svc/v1",
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("connect bridge via etcd resolver failed: %v", err)
	}
	defer conn.Close()

	// 4. 创建 Bridge 客户端
	client := bridgev1.NewBridgeServiceClient(conn)

	// 5. 构建业务请求
	req := &pb.HelloReq{
		Name: "daheige",
	}
	payload, err := anypb.New(req)
	if err != nil {
		log.Fatal(err)
	}

	// 6. 调用 Bridge
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
		// target: 下游服务名/方法名，对应 gRPC 方法路径
		Target: "Hello.Greeter/SayHello",
		// version: 下游 grpc protobuf 协议版本号，为空表示无版本
		Version: "",
		// protocol: 下游服务协议类型（GRPC/HTTP）
		Protocol: "GRPC",
		// payload: 业务请求负载
		Payload: payload,
		// metadata: 透传给下游的元数据
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

	// 7. 处理响应
	if resp.Status != nil && resp.Status.Code != 0 {
		fmt.Printf("Error: %s", resp.Status.Message)
		return
	}

	// 8. 从 Any 解包业务响应
	var res pb.HelloReply
	if err := resp.Payload.UnmarshalTo(&res); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("res message:%s\n", res.Message)
}
