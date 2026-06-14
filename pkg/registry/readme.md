# 服务注册与发现

`pkg/registry` 提供基于 etcd 的服务注册、发现与 gRPC resolver 能力。后续这个包，将独立到单个仓库中开源。

## 核心设计

- 每个服务实例注册到独立的 etcd key：
  ```
  /services/{service}/{version}/{instance-id}
  ```
  例如：
  ```
  /services/Hello.Greeter/v1/127.0.0.1-50051
  /services/Hello.Greeter/v1/127.0.0.1-50052
  ```
- `instance-id` 默认使用 `Endpoint.Address` 的 sanitized 值，也可通过 `WithInstanceID("id")` 显式指定。
- `version` 为空时，默认使用 `_default`。
- 发现端通过 prefix 查询聚合所有实例，支持 `Get` 与 `Watch`。

## gRPC 服务注册示例

```go
package main

import (
	"log"
	"net"

	"github.com/daheige/bridge-svc/pkg/registry"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	gs := grpc.NewServer()
	// register your gRPC service here...

	reg, err := registry.NewServiceRegistry(
		[]string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"},
		"/services/",
		"order_service",
		"v1",
		registry.Endpoint{
			Address:  "10.0.1.5:50051",
			Weight:   100,
			Protocol: "GRPC",
			Region:   "cn-north-1",
			Tags:     map[string]string{"version": "v1"},
			Healthy:  true,
		},
		registry.WithTTL(10),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := reg.Register(); err != nil {
		log.Fatal(err)
	}
	defer reg.Deregister()

	log.Println("Order service starting on :50051")
	if err := gs.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
```

## HTTP 服务注册示例

```go
package main

import (
	"log"
	"net/http"

	"github.com/daheige/bridge-svc/pkg/registry"
)

func main() {
	mux := http.NewServeMux()
	// register your handlers here...

	reg, err := registry.NewServiceRegistry(
		[]string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"},
		"/services/",
		"user_service",
		"v1",
		registry.Endpoint{
			Address:  "10.0.1.7:8080",
			Weight:   100,
			Protocol: "HTTP",
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

	log.Println("User service starting on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
```

## 服务发现

```go
discovery, err := registry.NewEtcdDiscovery(
	[]string{"127.0.0.1:2379"},
	"/services",
	5*time.Second,
)
if err != nil {
	log.Fatal(err)
}

// 一次性获取
endpoints, err := discovery.Get(context.Background(), "Hello.Greeter", "v1")
if err != nil {
	log.Fatal(err)
}

// 监听变化
watcher, err := discovery.Watch(context.Background(), "Hello.Greeter", "v1")
if err != nil {
	log.Fatal(err)
}
defer watcher.Stop()

for {
	endpoints, err := watcher.Next()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("updated endpoints:", endpoints)
}
```

## gRPC Resolver

客户端可以通过自定义 scheme 直接访问 etcd 发现的服务。

```go
discovery, err := registry.NewEtcdDiscovery(
	[]string{"127.0.0.1:2379"},
	"/services",
	5*time.Second,
)
if err != nil {
	log.Fatal(err)
}

// scheme 默认为 "etcd"
registry.RegisterEtcdResolver(discovery, "")

conn, err := grpc.NewClient(
	"etcd:///Hello.Greeter/v1",
	grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()
```

## 注意事项

- 包路径为 `pkg/registry`。
- `Endpoint.Address` 应为 `host:port` 格式，gRPC 场景不要带 `http://` 前缀。
- `version` 为空时，注册侧与发现侧都会规范化为 `_default`。
- 发现端 `Discovery.Get` / `Watch` 会按 `Address` 去重，不校验 `Healthy` 字段，请确保注册时按需设置健康状态并在业务侧过滤。
- `Deregister()` 幂等安全，程序优雅退出时建议通过 `defer` 调用。
