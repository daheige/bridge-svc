# bridge-svc

Go bridge service for gRPC and HTTP protocol proxy.

`bridge-svc` 作为业务方与下游微服务之间的统一代理层，对外暴露统一的 gRPC 接口，内部根据配置将请求路由并转发到下游 gRPC 或 HTTP 微服务。

## 核心特性

- **统一协议入口**：业务方通过 gRPC 协议调用 Bridge，无需关心下游协议差异。
- **动态服务发现**：基于 etcd 实现服务注册与发现，支持本地缓存 + Watch 增量更新。
- **多协议转发**：支持 `GRPC` 协议透传和 `HTTP` 协议转换。
- **零拷贝透传**：gRPC 场景使用 `raw-bytes` codec 直接透传业务消息字节，Bridge 不反序列化具体业务类型。
- **gRPC Reflection**：自动通过下游 gRPC Reflection 获取输出类型，填充响应 `Any.TypeUrl`，便于客户端 `UnmarshalTo`。
- **稳定性保障**：内置熔断器（按 endpoint 维度）、限流器、重试策略。
- **可观测性**：集成 OpenTelemetry Trace、Prometheus 指标和 zerolog 结构化日志。
- **服务注册 SDK**：`pkg/registry` 提供 etcd 服务注册、发现与自定义 gRPC resolver。

## 架构

详细架构设计与实现请参考 [bridge.md](bridge.md)。

```
Upstream (gRPC Client)
        |
        | gRPC
        v
Bridge Service (:50052)
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

- Go 1.25+
- etcd 3.5+
- protoc（可选，仅修改 proto 时需要）

### 构建

```bash
make build
```

### 生成 protobuf 代码

```bash
make proto
```

### 运行

```bash
# 1. 启动 etcd
# 2. 注册下游服务到 etcd（参考 pkg/registry/readme.md）
# 3. 启动 Bridge
make run
```

默认监听 `0.0.0.0:50052`，Prometheus 指标端口为 `9090`。

### 客户端调用

参考 [client/main.go](client/main.go)：

```go
resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
    Target:   "Hello.Greeter/SayHello/v1",
    Protocol: "GRPC",
    Payload:  payload,
    Metadata: map[string]string{
        "authorization": "Bearer token123",
    },
    TimeoutMs: 3000,
})
```

## 主要目录

```
bridge-svc/
├── api/v1              # Bridge gRPC API 定义
├── client              # 客户端调用示例
├── cmd/bridge          # 服务入口
├── config              # 运行时配置
├── internal            # 内部实现
│   ├── config          # 配置加载与热更新
│   ├── middleware      # 拦截器链
│   ├── observability   # Trace / Metrics / Logging
│   ├── pool            # gRPC 连接池
│   ├── protocol        # 协议处理器
│   ├── resilience      # 熔断 / 限流 / 重试
│   ├── router          # 路由与负载均衡
│   └── server          # gRPC Server 组装
├── k8s                 # Kubernetes 部署示例
└── pkg/registry        # 服务注册、发现与 gRPC resolver SDK
```

## 配置

配置文件位于 [config/bridge.yaml](config/bridge.yaml)，支持环境变量覆盖（前缀 `BRIDGE_`）。

## 下游服务注册

下游服务使用 `pkg/registry` 注册到 etcd：

```go
reg, err := registry.NewServiceRegistry(
    []string{"127.0.0.1:2379"},
    "/services/",
    "Hello.Greeter",
    "v1",
    registry.Endpoint{
        Address:  "127.0.0.1:50051",
        Weight:   100,
        Protocol: "GRPC",
        Healthy:  true,
    },
)
if err != nil { log.Fatal(err) }
if err := reg.Register(); err != nil { log.Fatal(err) }
defer reg.Deregister()
```

详细说明见 [pkg/registry/readme.md](pkg/registry/readme.md)。

## 调试

```bash
# 查看服务列表
grpcurl -plaintext localhost:50052 list

# 健康检查
grpcurl -plaintext localhost:50052 bridge.v1.BridgeService/Health

# 调用一元方法
grpcurl -plaintext -d '{
  "target": "Hello.Greeter/SayHello/v1",
  "protocol": "GRPC",
  "timeout_ms": 3000
}' localhost:50052 bridge.v1.BridgeService/CallUnary
```

## License

[MIT](LICENSE)
