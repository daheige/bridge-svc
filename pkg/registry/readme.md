# etcd实现服务注册和发现
    etcd是一个开源的、高可用的分布式key-value存储系统，可以用于配置共享和服务的注册和发现。

# 本地运行etcd
```shell
docker run -d \
  --name etcd_test \
  --restart=always \
  -p 12379:2379 \
  -p 12380:2380 \
  quay.io/coreos/etcd:v3.5.1 \
  /usr/local/bin/etcd \
  --name etcd_test \
  --data-dir /etcd-data \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379
```

# gRPC 下游服务启动示例

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/daheige/bridge-svc/pkg/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// OrderService 订单服务实现
type OrderService struct {
	UnimplementedOrderServiceServer
}

// CreateOrder 创建订单方法
// Bridge 会将业务方的请求路由到此方法
func (s *OrderService) CreateOrder(ctx context.Context, req *CreateOrderRequest) (*CreateOrderResponse, error) {
	// 业务逻辑处理
	orderID := fmt.Sprintf("order-%d", time.Now().Unix())
	return &CreateOrderResponse{
		OrderId: orderID,
		Status:  "created",
	}, nil
}

func main() {
	// 1. 创建 gRPC 服务
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	gs := grpc.NewServer()
	RegisterOrderServiceServer(gs, &OrderService{})
	reflection.Register(gs) // 启用反射，便于 Bridge 调用

	// 2. 注册到 etcd（关键步骤）
	reg, err := registry.NewServiceRegistry(
		[]string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"},
		"/services/",           // etcd 前缀

		"Hello.Greeter",        // 服务名（对应 gRPC full method name 的 service 部分）
		"default",              // 版本
		registry.Endpoint{
			Address:  "10.0.1.5:50051",  // 本机地址
			Weight:   100,
			Protocol: "GRPC",             // 协议类型
			Region:   "cn-north-1",
			Tags:     map[string]string{"version": "v1"},
			Healthy:  true,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := reg.Register(); err != nil {
		log.Fatal(err)
	}
	defer reg.Deregister() // 优雅退出时注销

	// 3. 启动 gRPC 服务
	log.Println("Order service starting on :50051")
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// 4. 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down order service...")
}
```

### HTTP 下游服务启动示例

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daheige/bridge-svc/pkg/registry"
)

// UserHandler 用户服务 HTTP 处理器
type UserHandler struct{}

// GetUser 获取用户信息
// Bridge 会将 gRPC 请求转换为 HTTP 请求路由到此方法
func (h *UserHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Path[len("/user_service/"):]
	resp := map[string]interface{}{
		"user_id": userID,
		"name":    "张三",
		"email":   "zhangsan@example.com",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	// 1. 创建 HTTP 服务
	mux := http.NewServeMux()
	mux.HandleFunc("/user_service/GetUser", func(w http.ResponseWriter, r *http.Request) {
		// Bridge 会将请求路径映射为 /user_service/GetUser
		userID := r.Header.Get("x-user-id") // 从 metadata 透传的 header
		resp := map[string]interface{}{
			"user_id": userID,
			"name":    "张三",
			"email":   "zhangsan@example.com",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// 2. 注册到 etcd（关键步骤）
	reg, err := registry.NewServiceRegistry(
		[]string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"},
		"/services/",           // etcd 前缀
		"Hello.World",          // 服务名（对应 gRPC full method name 的 service 部分）
		"default",              // 版本
		registry.Endpoint{
			Address:  "10.0.1.7:8080",   // 本机地址
			Weight:   100,
			Protocol: "HTTP",             // 协议类型（Bridge 使用 HTTP Handler）
			Region:   "cn-north-1",
			Tags:     map[string]string{"version": "v1"},
			Healthy:  true,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := reg.Register(); err != nil {
		log.Fatal(err)
	}
	defer reg.Deregister()

	// 3. 启动 HTTP 服务
	log.Println("User service starting on :8080")
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// 4. 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 5. 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	log.Println("User service stopped")
}
```

### 业务方调用示例（对应上述服务）

```go
// 调用 Greeter 服务（gRPC 下游）
// Target 对应 etcd 中的 /services/Hello.Greeter
resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
	Target:   "Hello.Greeter/SayHello",  // gRPC full method name（去掉开头的 /）
	Protocol: "GRPC",
	Payload:  helloPayload,
	Metadata: map[string]string{
		"authorization": "Bearer token123",
	},
})

// 调用 World 服务（HTTP 下游）
// 对应 etcd 中的 /services/Hello.World/default
resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
	Target:   "Hello.World/Hello",         // gRPC full method name 格式
	Protocol: "HTTP",
	Payload:  helloPayload,
	Metadata: map[string]string{
		"x-user-id": "user-456",
	},
})
```

### etcd 数据结构说明

```
etcd 键值结构：

/services/                    # 前缀（Bridge 配置中的 etcd.prefix）
  ├── order_service/          # 服务名（对应 Bridge target 的第一部分）
  │     └── default           # 版本（对应 Bridge target 的第二个部分，省略时为 default）
  │           → [{"address":"10.0.1.5:50051","weight":100,"protocol":"GRPC",...}]
  │
  ├── payment_service/
  │     └── v2                # 版本号（对应 target: "payment_service/v2/Charge"）
  │           → [{"address":"10.0.1.6:50051","weight":100,"protocol":"GRPC",...}]
  │
  └── user_service/
		└── default
			  → [{"address":"10.0.1.7:8080","weight":100,"protocol":"HTTP",...}]
```

**target 解析规则**：
- `"order_service/CreateOrder"` → service=`order_service`, version=`default`, method=`CreateOrder`
- `"payment_service/v2/Charge"`   → service=`payment_service`, version=`v2`, method=`Charge`

### grpcurl 调试命令

```bash
# 查看 Bridge 服务列表
grpcurl -plaintext bridge-svc:50051 list

# 调用健康检查
grpcurl -plaintext bridge-svc:50051 bridge.v1.BridgeService/Health

# 调用一元方法（需要准备 payload）
grpcurl -plaintext -d '{
  "target": "order_service/CreateOrder",
  "protocol": "GRPC",
  "timeout_ms": 3000,
  "metadata": {
	"authorization": "Bearer token123",
	"x-request-id": "req-456"
  }
}' bridge-svc:50051 bridge.v1.BridgeService/CallUnary
```

