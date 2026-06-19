# bridge-svc

Go bridge service for gRPC and HTTP protocol proxy.

`bridge-svc` 作为业务方与下游微服务之间的统一代理层，对外暴露统一的 gRPC 接口，内部根据配置将请求路由并转发到下游 gRPC 或 HTTP 微服务。

## 核心特性

- **统一协议入口**：业务方通过 gRPC 协议调用 Bridge，无需关心下游协议差异。
- **动态服务发现**：基于 etcd 实现服务注册与发现，支持本地缓存 + Watch 增量更新。
- **多协议转发**：支持 `GRPC` 协议透传和 `HTTP` 协议转换。
- **零拷贝透传**：gRPC 场景直接透传业务消息字节，Bridge 不反序列化具体业务类型。
- **gRPC Reflection**：自动通过下游 gRPC Reflection 获取输出类型，填充响应 `Any.TypeUrl`，便于客户端 `UnmarshalTo`。
- **稳定性保障**：内置熔断器（按 endpoint 维度）、限流器、重试策略。
- **可观测性**：集成 OpenTelemetry Trace、Prometheus 指标和 zerolog 结构化日志。
- **自注册与发现**：Bridge 启动时自动注册到 etcd，客户端可通过 etcd resolver 动态发现 Bridge 节点。
- **配置热更新**：基于 viper 的配置文件监听，修改后自动生效（无需重启）。

## 架构

详细架构设计与实现请参考 [bridge.md](bridge.md)。

```
Upstream (gRPC Client)
        |
        | gRPC
        v
Bridge Service (:50052 本地 / :50051 容器/K8s)
        |
        | etcd discovery + route
        v
Protocol Handler (gRPC / HTTP)
        |
        | gRPC / HTTP
        v
Downstream Microservice
```

## 快速开始

### 前置依赖

- Go 1.25+（`go.mod` 指定 `go 1.25.9`）
- etcd 3.5+
- protoc（可选，仅修改 proto 时需要）

### 构建

```bash
make build
```

### 生成 protobuf 代码

proto 文件位于 `api/v1/bridge.proto`，生成命令：

```bash
make proto
```

实际执行的命令：

```bash
protoc -I api/v1 \
    --go_out=api/v1 --go_opt=paths=source_relative \
    --go-grpc_out=api/v1 --go-grpc_opt=paths=source_relative \
    api/v1/bridge.proto
```

### 运行

```bash
# 1. 启动 etcd
# 2. 注册下游服务到 etcd（参考 github.com/daheige/registry）
# 3. 启动 Bridge
make run
```

本地默认监听 `0.0.0.0:50052`，Prometheus 指标端口为 `9090`。启动后 Bridge 会自动注册到 etcd，注册路径由 `config/bridge.yaml` 中的 `etcd.prefix` 和 `server.service_name` 决定。

> **注意**：Dockerfile 与 K8s 清单中默认暴露/使用 gRPC 端口 `50051`，与本地 `config/bridge.yaml` 的 `50052` 不同。容器化部署时请通过环境变量或挂载配置统一端口。

### 客户端调用

参考 [client/main.go](client/main.go)：

```go
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
        // 关键配置：启用 round_robin 负载均衡策略
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

    // 3. 构建业务请求
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
```

响应包含以下字段：

- `payload`：下游返回的 `google.protobuf.Any`。
- `metadata`：从下游透传的响应元数据。
- `status`：`google.rpc.Status`，失败时非空。
- `upstream_node`：实际调用的下游节点地址。
- `latency_us`：Bridge 内部处理耗时（微秒）。

如需通过 etcd 动态发现 Bridge 服务，参考 [client/resolver/main.go](client/resolver/main.go)：

```go
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

    "github.com/daheige/registry/etcd"

    bridgev1 "github.com/daheige/bridge-svc/api/v1"
)

func main() {
    // 1. 创建 etcd 服务发现器
    // 实际使用时应从配置文件读取 etcd 地址与前缀。
    discovery, err := etcd.NewEtcdDiscovery(
        []string{"localhost:12379"}, // etcd 集群地址
        "/services",                 // 服务注册前缀
        5*time.Second,               // 连接超时
    )
    if err != nil {
        log.Fatalf("create etcd discovery failed: %v", err)
    }

    // 2. 注册 etcd gRPC resolver，scheme 为空则默认使用 "etcd"
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
```

> 注意：resolver 示例中使用前缀 `/services`（无末尾斜杠），而 Bridge 默认配置为 `/services/`。实际使用时请与下游服务注册时使用的 prefix 保持一致。

## 主要目录

```
bridge-svc/
├── api/v1              # Bridge gRPC API 定义
├── client              # 客户端调用示例
│   └── resolver        # 基于 etcd resolver 发现 Bridge 的示例
├── cmd/bridge          # 服务入口
├── config              # 运行时配置
├── internal            # 内部实现
│   ├── config          # 配置加载（支持热更新）
│   ├── middleware      # 拦截器链（Recovery / Logging / Auth / RateLimit）
│   ├── observability   # Trace / Metrics / Logging
│   ├── pool            # gRPC 连接池
│   ├── protocol        # 协议处理器
│   ├── resilience      # 熔断 / 限流 / 重试
│   ├── router          # 路由与负载均衡
│   └── server          # gRPC Server 组装
├── k8s                 # Kubernetes 部署示例
└── bin                 # 构建产物
```

## 配置

配置文件位于 [config/bridge.yaml](config/bridge.yaml)，支持环境变量覆盖（前缀 `BRIDGE_`，通过 viper 自动映射）。

### 主要配置项

```yaml
server:
  listen_addr: "0.0.0.0:50052"       # gRPC 监听地址
  max_concurrent_streams: 10000       # 最大并发流
  keepalive_time: 7200s               # 连接保活时间
  keepalive_timeout: 20s              # 保活超时
  service_name: "bridge-svc"          # Bridge 服务名（用于 etcd 自注册）
  service_version: "v1"               # Bridge 服务版本

etcd:
  endpoints:                          # etcd 集群地址
    - "http://127.0.0.1:12379"
  dial_timeout: 5s                    # 连接超时
  prefix: "/services/"                # 服务注册前缀

protocols:
  default_timeout: 30s                # Bridge 到下游的默认超时
  grpc:
    max_send_msg_size: 16777216       # 16MB（下游 gRPC 发送限制）
    max_recv_msg_size: 16777216       # 16MB（下游 gRPC 接收限制）
    dial_timeout: 10s                 # 连接下游 gRPC 超时
  http:
    max_conns_per_host: 100           # 每下游主机最大连接数
    read_timeout: 10s                 # 下游 HTTP 读超时
    write_timeout: 10s                # 下游 HTTP 写超时

resilience:
  circuit_breaker:
    max_requests: 3                   # 半开状态最大请求数
    interval: 10s                     # 统计周期
    timeout: 30s                      # 熔断持续时间
    failure_ratio: 0.6                # 触发熔断的失败率阈值
    success_ratio: 0.5                # 恢复熔断的成功率阈值
    half_open_max: 1                  # 半开状态最大探测请求
  rate_limiter:
    rps: 10000                        # 每秒请求数（限流）
    burst_size: 500                   # 突发容量
  retry:
    max_attempts: 3                   # 最大重试次数
    initial_backoff: 100ms            # 初始退避时间
    max_backoff: 5s                   # 最大退避时间
    backoff_multiplier: 2.0           # 退避乘数

observability:
  log_level: "info"                   # 日志级别
  trace_endpoint: "localhost:5081"    # OTLP Trace 接收端（为空则关闭 Trace）
  metrics_port: 9090                  # Prometheus 指标端口
```

### 环境变量示例

容器或 K8s 中常用环境变量：

```bash
export BRIDGE_SERVER_LISTEN_ADDR="0.0.0.0:50051"
export BRIDGE_ETCD_ENDPOINTS="etcd-0:2379,etcd-1:2379,etcd-2:2379"
export BRIDGE_OBSERVABILITY_TRACE_ENDPOINT="otel-collector.monitoring:4317"
export BRIDGE_OBSERVABILITY_LOG_LEVEL="info"
```

## 拦截器链

当前默认启用的拦截器顺序（从外到内）：

1. **RecoveryInterceptor**：捕获 panic，防止单个请求拖垮进程。
2. **LoggingInterceptor**：结构化日志记录每个请求的 method、耗时、metadata 和错误。

以下拦截器已实现但默认未启用，可按需打开：

- **AuthInterceptor**：占位实现，校验 `authorization` metadata 格式，可扩展 JWT / Casbin。
- **RateLimitInterceptor**：令牌桶限流，保护下游服务。
- **TraceInterceptor**：OpenTelemetry Trace 上下文透传（当前通过 `ExtractTraceContext` 在可观测性层处理）。

## 可观测性

### Prometheus 指标

指标通过 `http://<host>:9090/metrics` 暴露，当前内置指标：

| 指标名 | 类型 | 说明 |
| --- | --- | --- |
| `bridge_request_total` | CounterVec | 请求总数，按 `method`、`status` 统计 |
| `bridge_request_duration_seconds` | HistogramVec | 请求耗时，按 `method` 统计 |
| `bridge_active_connections` | Gauge | 当前与业务方建立的 gRPC 连接数 |

### OpenTelemetry Trace

- 通过 `observability.trace_endpoint` 配置 OTLP gRPC 接收端。
- 支持从上游 gRPC metadata 提取 trace context，实现链路透传。
- 未配置 trace endpoint 时自动降级为 noop tracer。

### 日志

使用 zerolog 输出结构化 JSON 日志，日志级别由 `observability.log_level` 控制。

## 下游服务注册

下游服务使用独立的 `github.com/daheige/registry` 库注册到 etcd：

```go
import (
    "github.com/daheige/registry"
    "github.com/daheige/registry/etcd"
)

reg, err := etcd.NewServiceRegistry(
    []string{"127.0.0.1:2379"},
    "/services/",
    "Hello.Greeter",
    registry.Endpoint{
        Address:  "127.0.0.1:50051",
        Weight:   100,
        Protocol: registry.ProtocolGRPC,
        Version:  "v1",
        Healthy:  true,
    },
)
if err != nil { log.Fatal(err) }
if err := reg.Register(); err != nil { log.Fatal(err) }
defer reg.Deregister()
```

> 下游 gRPC 服务需开启 gRPC Reflection，Bridge 才能正确填充响应 `Any.TypeUrl`。如果下游未开启反射，客户端仍可通过 `proto.Unmarshal(resp.Payload.Value, &res)` 绕过类型校验。

## 部署与运维

### Docker

项目提供 [Dockerfile](Dockerfile)，基于 Go 1.24 构建（ alpine 多阶段）：

```bash
make docker
```

构建后镜像默认暴露端口 `50051`（gRPC）和 `9090`（metrics）。运行示例：

```bash
docker build -t bridge-svc:latest .
docker run -d \
  -p 50051:50051 \
  -p 9090:9090 \
  -e BRIDGE_SERVER_LISTEN_ADDR="0.0.0.0:50051" \
  -e BRIDGE_ETCD_ENDPOINTS="http://host.docker.internal:12379" \
  bridge-svc:latest
```

### Kubernetes

参考 [k8s/deployment.yaml](k8s/deployment.yaml)，包含 Deployment、Service 和 ConfigMap：

```bash
kubectl apply -f k8s/deployment.yaml
```

关键配置：

- namespace：`infrastructure`
- 副本数：`3`
- 容器端口：`50051`（gRPC）、`9090`（metrics）
- 存活探针/就绪探针：gRPC 健康检查，端口 `50051`
- Prometheus 自动抓取注解：`prometheus.io/scrape: "true"`、`prometheus.io/port: "9090"`
- 资源配置：requests `512Mi/500m`，limits `2Gi/2000m`
- 环境变量通过 `BRIDGE_*` 覆盖 ConfigMap 中的配置

### 健康检查

```bash
# 查看服务列表
grpcurl -plaintext localhost:50052 list

# 健康检查
grpcurl -plaintext localhost:50052 bridge.v1.BridgeService/Health

# 调用一元方法
grpcurl -plaintext -d '{
  "target": "Hello.Greeter/SayHello",
  "version": "",
  "protocol": "GRPC",
  "timeout_ms": 3000
}' localhost:50052 bridge.v1.BridgeService/CallUnary
```

> 容器/K8s 环境中请将 `localhost:50052` 替换为实际端口（如 `localhost:50051`）。

## License

[MIT](LICENSE)
