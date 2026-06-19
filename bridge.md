# 【go grpc bridge服务架构设计与实现】

> **版本**: v3.1 (2026-06-14)  
> **Go版本**: 1.25.8+  
> **依赖更新**: 依赖版本与 `go.mod` 当前状态保持一致

---

## 目录

1. [架构概述](#1-架构概述)
2. [项目结构](#2-项目结构)
3. [Protobuf 定义](#3-protobuf-定义)
4. [核心模块实现](#4-核心模块实现)
5. [服务入口与启动](#5-服务入口与启动)
6. [配置文件](#6-配置文件)
7. [Makefile 与构建](#7-makefile-与构建)
8. [部署方案](#8-部署方案)
9. [调用方式与接入指南](#9-调用方式与接入指南)
10. [性能优化清单](#10-性能优化清单)
11. [附录：关键设计决策](#11-附录关键设计决策)

---

## 1. 架构概述

### 1.1 设计目标

Bridge 服务作为**业务方与下游微服务之间的统一代理层**，承担以下核心职责：

- **统一协议入口**：业务方通过 **gRPC 协议** 调用 Bridge，Bridge 负责转发到下游不同协议的微服务
- **动态路由**：基于 etcd 服务发现，支持权重、灰度、区域亲和
- **稳定性保障**：熔断、限流、重试、超时，防止级联故障
- **协议透明**：Bridge 对外统一暴露 gRPC 接口，内部根据配置转发到下游 gRPC/HTTP 服务
- **可观测性**：全链路 Trace + 结构化日志 + Prometheus 指标，直连 OpenObserve

### 1.2 整体架构

```mermaid
graph TB
    subgraph "业务方 (Upstream)"
        A[业务服务A_Go_gRPC]
        B[业务服务B_Java_gRPC]
        C[业务服务C_Python_gRPC]
    end

    subgraph "Bridge Service (Go 1.24 + grpc-go v1.79)"
        D[grpc_Server_50052]
        E[Interceptor_Chain]
        F[Router_Engine]
        G[Protocol_Adapters]
        H[Resilience_Layer]
        I[Observability]
    end

    subgraph "Service Discovery"
        J[etcd_Cluster]
    end

    subgraph "下游微服务 (Downstream)"
        K[订单服务_gRPC]
        L[支付服务_gRPC]
        M[用户服务_HTTP]
        N[库存服务_HTTP]
    end

    subgraph "OpenObserve Platform"
        O[OpenObserve_Platform]
    end

    A -- gRPC --> D
    B -- gRPC --> D
    C -- gRPC --> D
    D --> E
    E -- RouteDecision --> F
    F -- Lookup --> J
    F --> G
    G -- ConnMgmt --> H
    H --> K
    H --> L
    H --> M
    H --> N
    H --> O
    D -.-> O
    I -.-> O
```

### 1.3 核心数据流

```mermaid
sequenceDiagram
    participant C as 业务方 (gRPC Client)
    participant B as BridgeService (gRPC Server)
    participant R as Router
    participant CB as CircuitBreaker
    participant H as ProtocolHandler
    participant S as 下游微服务

    C->>B: 1. CallUnary(target, payload, metadata)
    Note over C,B: gRPC协议通信

    B->>B: 2. time.Now() 开始计时

    B->>R: 3. Route(ctx, {Target, Protocol})
    R-->>B: 4. RouteTarget{Endpoint: "10.0.1.5:50051"}

    B->>CB: 5. Execute(endpoint, fn)
    CB->>CB: 6. 检查状态 (Closed/Open/HalfOpen)

    alt 熔断器Open
        CB-->>B: 6a  直接返回 error
        B-->>C: 6b  Status{Code: UNAVAILABLE}
    else 熔断器Closed
        CB->>H: 7. handler.Call(ctx, target, payload, timeout)
        H->>S: 8. 协议转发 (gRPC/HTTP)
        S-->>H: 9. Response
        H-->>CB: 10. 返回 result
        CB->>CB: 11. 记录成功/失败
        CB-->>B: 12. result
        B->>B: 13. 计算 latency_us
        B-->>C: 14. UnaryResponse{payload, latency_us, upstream_node}
    end
```

### 1.4 技术选型（2026年最新版本）

| 组件 | 选型 | 版本 | 理由 |
|------|------|------|------|
| gRPC 框架 | `google.golang.org/grpc` | **v1.81.1+** | 当前 go.mod 锁定版本 |
| Protobuf | `google.golang.org/protobuf` | **v1.36.11+** | 与 grpc-go v1.81 兼容 |
| etcd 客户端 | `go.etcd.io/etcd/client/v3` | **v3.6.12+** | 当前 go.mod 锁定版本 |
| 熔断器 | `github.com/sony/gobreaker/v2` | **v2.4.0** | 支持泛型，按 endpoint 维度隔离 |
| 限流 | `golang.org/x/time/rate` | **v0.12.0+** | 标准扩展 |
| 可观测性 | `go.opentelemetry.io/otel` | **v1.44.0+** | OTLP Trace 导出 |
| 日志 | `github.com/rs/zerolog` | **v1.35.1+** | 零分配 JSON 日志 |
| HTTP 客户端 | `github.com/go-resty/resty/v2` | **v2.17.2+** | 支持中间件、重试、拦截器 |
| 配置 | `github.com/spf13/viper` | **v1.21.0+** | 支持热更新 |
| Prometheus 客户端 | `github.com/prometheus/client_golang` | **v1.20.5+** | 指标采集 |
| Go 版本 | `Go` | **1.25.8+** | 当前 go.mod 声明版本 |

---

## 2. 项目结构

```
bridge-svc/
├── api/
│   └── v1/
│       ├── bridge.proto          # Bridge 对外 gRPC API 定义
│       ├── bridge.pb.go          # protoc-gen-go 生成
│       └── bridge_grpc.pb.go     # protoc-gen-go-grpc 生成
├── client/
│   └── main.go                   # 业务方调用 Bridge 的 Go 客户端示例
├── cmd/
│   └── bridge/
│       └── main.go               # 服务入口
├── internal/
│   ├── config/
│   │   └── config.go             # 配置结构 + 热更新
│   ├── server/
│   │   └── server.go             # grpc.Server 组装 + BridgeService 实现
│   ├── router/
│   │   ├── router.go             # 路由决策（支持 target + version）
│   │   ├── discovery.go          # （保留目录，实际发现逻辑在 router/registry）
│   │   ├── cache.go              # （保留目录）
│   │   └── balancer.go           # 加权轮询负载均衡
│   ├── protocol/
│   │   ├── protocol.go           # 协议处理器接口
│   │   ├── grpc.go               # gRPC → gRPC 透传（raw-bytes codec + reflection）
│   │   └── http.go               # gRPC → HTTP 转换（使用 resty/v2）
│   ├── resilience/
│   │   ├── circuit.go            # 熔断器管理
│   │   ├── retry.go              # 重试策略
│   │   └── ratelimit.go          # 限流器
│   ├── pool/
│   │   └── grpcpool.go           # gRPC 连接管理
│   ├── observability/
│   │   ├── tracing.go            # OpenTelemetry Trace
│   │   ├── metrics.go            # Prometheus 指标
│   │   ├── logging.go            # zerolog 初始化
│   │   └── init.go               # 可观测性统一初始化
│   └── middleware/
│       ├── chain.go              # 拦截器链组装
│       ├── auth.go               # 认证拦截器（占位）
│       ├── recovery.go           # Panic 恢复
│       ├── logging.go            # 日志拦截器
│       └── timeout.go            # 超时拦截器（占位）
├── pkg/
│   ├── registry/
│   │   ├── registry.go           # etcd 服务注册器（带自动重注册）
│   │   ├── discovery.go          # etcd 服务发现（Get + Watch）
│   │   ├── resolver.go           # 自定义 gRPC resolver（scheme: etcd）
│   │   └── readme.md             # 服务注册使用说明
│   └── utils/
│       └── any.go                # protobuf Any 工具函数
├── config/
│   └── bridge.yaml               # 运行时配置
├── go.mod
├── go.sum
├── Makefile
├── Dockerfile
└── k8s/
    └── deployment.yaml           # Kubernetes 部署示例
```

---

## 3. Protobuf 定义

### 3.1 `api/v1/bridge.proto`

```protobuf
syntax = "proto3";
package bridge.v1;

import "google/protobuf/any.proto";
import "google/rpc/status.proto";

option go_package = "github.com/daheige/bridge-svc/api/v1;bridgev1";

// 自定义 Status 消息
// 无需外部 googleapis 依赖

// Bridge 对外暴露的统一 gRPC 调用接口
// 业务方通过 gRPC 调用 Bridge，Bridge 负责转发到下游微服务
service BridgeService {
  // 一元调用：适用于大多数业务请求
  rpc CallUnary(UnaryRequest) returns (UnaryResponse);

  // 流式调用：适用于大数据量或实时推送场景（预留）
  rpc CallStream(stream StreamRequest) returns (stream StreamResponse);

  // 健康检查
  rpc Health(HealthRequest) returns (HealthResponse);
}

// 统一请求模型
// 业务方通过 gRPC 发送此请求，Bridge 解析 target 后路由到下游
message UnaryRequest {
  // 目标服务标识，格式: "PackageName.ServiceName/MethodName"
  // 对应 gRPC 的 full method name，例如: "Hello.Greeter/SayHello"
  // Bridge 解析后，使用 "PackageName.ServiceName" 作为 etcd 服务名，"MethodName" 作为方法名
  string target = 1;

  // 下游 grpc protobuf 协议版本号，例如：v1, v2
  // 为空表示无版本，仅按 target 进行路由
  string version = 2;

  // 下游协议类型：GRPC / HTTP
  // Bridge 根据此字段选择对应的协议处理器转发请求
  string protocol = 3;

  // 业务负载，使用 Any 实现零拷贝透传
  // 业务方将原始 protobuf Message 打包为 Any，Bridge 不感知具体 Schema
  google.protobuf.Any payload = 4;

  // 调用元数据，透传给下游微服务（如 auth token、trace context、region 等）
  map<string, string> metadata = 5;

  // 超时配置（毫秒），覆盖全局默认值
  // 控制 Bridge 到下游微服务的调用超时
  uint32 timeout_ms = 6;

  // 重试策略
  RetryPolicy retry = 7;
}

message RetryPolicy {
  uint32 max_attempts = 1;       // 最大重试次数
  uint32 initial_backoff_ms = 2;   // 初始退避时间
  float backoff_multiplier = 3;    // 退避乘数
  repeated int32 retryable_codes = 4; // 可重试的 gRPC 状态码
}

message UnaryResponse {
  // 成功响应负载（下游微服务返回的数据）
  google.protobuf.Any payload = 1;

  // 响应元数据（从下游微服务透传回来）
  map<string, string> metadata = 2;

  // 如果调用失败，使用自定义 Status 错误模型
  // 包含路由失败、熔断打开、下游超时等错误
  google.rpc.Status status = 3;

  // 实际调用的下游微服务节点（用于调试和链路追踪）
  string upstream_node = 4;

  // 调用耗时（微秒），从 Bridge 接收到请求到返回响应的总时间
  uint64 latency_us = 5;
}

message StreamRequest {
  // 目标服务标识，格式: "PackageName.ServiceName/MethodName"
  string target = 1;

  // 下游 grpc protobuf 协议版本号，例如：v1, v2
  string version = 2;

  string protocol = 3;
  google.protobuf.Any payload = 4;
  map<string, string> metadata = 5;
}

message StreamResponse {
  google.protobuf.Any payload = 1;
  map<string, string> metadata = 2;
  google.rpc.Status status = 3;
}

message HealthRequest {
  string service = 1; // 空则检查 Bridge 自身
}

message HealthResponse {
  enum ServingStatus {
    UNKNOWN = 0;
    SERVING = 1;
    NOT_SERVING = 2;
  }
  ServingStatus status = 1;
  repeated string checked_services = 2;
}
```

### 3.2 生成命令（2026年最新工具链）

```bash
# 安装 protoc 插件（2026年最新版本）
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# 生成 Go 代码
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  api/v1/bridge.proto
```

---

## 4. 核心模块实现

### 4.1 `go.mod`（2026年最新依赖版本）

```go
module github.com/daheige/bridge-svc

go 1.25.8

require (
    google.golang.org/grpc v1.81.1
    google.golang.org/protobuf v1.36.11
    google.golang.org/genproto/googleapis/rpc v0.0.0-20260610212136-7ab31c22f7ad

    go.etcd.io/etcd/client/v3 v3.6.12

    go.opentelemetry.io/otel v1.44.0
    go.opentelemetry.io/otel/sdk v1.44.0
    go.opentelemetry.io/otel/trace v1.44.0
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0

    github.com/sony/gobreaker/v2 v2.4.0
    github.com/rs/zerolog v1.35.1
    github.com/spf13/viper v1.21.0
    github.com/go-resty/resty/v2 v2.17.2
    github.com/prometheus/client_golang v1.20.5
    golang.org/x/time v0.12.0
    github.com/google/uuid v1.6.0
    github.com/fsnotify/fsnotify v1.10.1
)
```

### 4.2 `internal/config/config.go`

```go
package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Config 全局配置结构
type Config struct {
	Server        ServerConfig        `mapstructure:"server"`
	Etcd          EtcdConfig          `mapstructure:"etcd"`
	Protocols     ProtocolConfig      `mapstructure:"protocols"`
	Resilience    ResilienceConfig    `mapstructure:"resilience"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

// ServerConfig gRPC 服务器配置（业务方通过此端口调用 Bridge）
type ServerConfig struct {
	ListenAddr           string        `mapstructure:"listen_addr"`            // 监听地址，如 "0.0.0.0:50051"
	MaxConcurrentStreams uint32        `mapstructure:"max_concurrent_streams"` // 最大并发流
	KeepaliveTime        time.Duration `mapstructure:"keepalive_time"`         // 连接保活时间
	KeepaliveTimeout     time.Duration `mapstructure:"keepalive_timeout"`      // 保活超时
}

// EtcdConfig etcd 服务发现配置
type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`   // etcd 集群地址
	DialTimeout time.Duration `mapstructure:"dial_timeout"` // 连接超时
	Prefix      string        `mapstructure:"prefix"`       // 服务注册前缀
}

// ProtocolConfig 协议层配置（Bridge 到下游微服务的协议配置）
type ProtocolConfig struct {
	DefaultTimeout time.Duration `mapstructure:"default_timeout"` // 默认超时
	Grpc           GrpcConfig    `mapstructure:"grpc"`          // gRPC 下游配置
	HTTP           HTTPConfig    `mapstructure:"http"`          // HTTP 下游配置
}

// GrpcConfig gRPC 客户端配置（Bridge 作为客户端连接下游 gRPC 服务）
type GrpcConfig struct {
	MaxSendMsgSize int           `mapstructure:"max_send_msg_size"` // 最大发送消息大小
	MaxRecvMsgSize int           `mapstructure:"max_recv_msg_size"` // 最大接收消息大小
	DialTimeout    time.Duration `mapstructure:"dial_timeout"`      // 连接超时
}

// HTTPConfig HTTP 客户端配置（Bridge 作为客户端连接下游 HTTP 服务）
type HTTPConfig struct {
	MaxConnsPerHost int           `mapstructure:"max_conns_per_host"` // 每主机最大连接数
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`       // 读超时
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`      // 写超时
}

// ResilienceConfig 稳定性保障配置
type ResilienceConfig struct {
	CircuitBreaker CircuitBreakerConfig `mapstructure:"circuit_breaker"` // 熔断器
	RateLimiter    RateLimiterConfig    `mapstructure:"rate_limiter"`    // 限流器
	Retry          RetryConfig          `mapstructure:"retry"`           // 重试策略
}

// CircuitBreakerConfig 熔断器配置
type CircuitBreakerConfig struct {
	MaxRequests  uint32        `mapstructure:"max_requests"`  // 半开状态最大请求数
	Interval     time.Duration `mapstructure:"interval"`      // 统计周期
	Timeout      time.Duration `mapstructure:"timeout"`       // 熔断持续时间
	FailureRatio float64       `mapstructure:"failure_ratio"` // 触发熔断的失败率阈值
	SuccessRatio float64       `mapstructure:"success_ratio"` // 恢复熔断的成功率阈值
	HalfOpenMax  uint32        `mapstructure:"half_open_max"` // 半开状态最大探测请求
}

// RateLimiterConfig 限流器配置
type RateLimiterConfig struct {
	RPS       int `mapstructure:"rps"`        // 每秒请求数
	BurstSize int `mapstructure:"burst_size"` // 突发容量
}

// RetryConfig 重试策略配置
type RetryConfig struct {
	MaxAttempts       uint32        `mapstructure:"max_attempts"`       // 最大重试次数
	InitialBackoff    time.Duration `mapstructure:"initial_backoff"`    // 初始退避时间
	MaxBackoff        time.Duration `mapstructure:"max_backoff"`        // 最大退避时间
	BackoffMultiplier float64       `mapstructure:"backoff_multiplier"` // 退避乘数
}

// ObservabilityConfig 可观测性配置
type ObservabilityConfig struct {
	LogLevel       string `mapstructure:"log_level"`       // 日志级别
	TraceEndpoint  string `mapstructure:"trace_endpoint"`  // OTLP Trace 接收端
	MetricsPort    int    `mapstructure:"metrics_port"`    // Prometheus 指标端口
	ServiceName    string `mapstructure:"service_name"`    // 服务名
	ServiceVersion string `mapstructure:"service_version"` // 服务版本
}

var (
	instance *Config
	mu       sync.RWMutex
)

// Load 加载配置文件，支持热更新
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("BRIDGE")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// 热更新监听
	v.OnConfigChange(func(in fsnotify.Event) {
		var newCfg Config
		if err := v.Unmarshal(&newCfg); err != nil {
			log.Error().Err(err).Msg("config hot reload failed")
			return
		}
		mu.Lock()
		instance = &newCfg
		mu.Unlock()
		log.Info().Msg("config reloaded")
	})
	v.WatchConfig()

	instance = &cfg
	return &cfg, nil
}

// Get 获取当前配置（线程安全）
func Get() *Config {
	mu.RLock()
	defer mu.RUnlock()
	return instance
}
```

### 4.3 `internal/router/router.go`

```go
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"

	"github.com/daheige/bridge-svc/internal/config"
)

// ProtocolType 下游微服务协议类型
type ProtocolType string

const (
	ProtocolGRPC ProtocolType = "GRPC" // 下游是 gRPC 服务
	ProtocolHTTP ProtocolType = "HTTP" // 下游是 HTTP/REST 服务
)

// Endpoint 下游微服务节点
type Endpoint struct {
	Address  string            `json:"address"`  // 下游服务地址，如 "10.0.1.5:50051"
	Weight   uint32            `json:"weight"`   // 权重，用于负载均衡
	Protocol ProtocolType      `json:"protocol"` // 协议类型
	Region   string            `json:"region"`   // 区域，如 "cn-north-1"
	Tags     map[string]string `json:"tags"`     // 标签，如 {"version": "v2"}
	Healthy  bool              `json:"healthy"`  // 健康状态
}

// RouteContext 路由上下文（从业务方请求中提取）
type RouteContext struct {
	Target       string       // 目标服务路径，如 "order_service/CreateOrder" 或 "order_service/CreateOrder/v2"
	Protocol     ProtocolType // 协议类型
	Metadata     metadata.MD  // 元数据，透传给下游
	PreferRegion string       // 优先区域
	Canary       string       // 灰度标识
}

// RouteTarget 路由结果（选中的下游微服务节点）
type RouteTarget struct {
	ServiceName string   // 服务名，如 "order_service"
	MethodName  string   // 方法名，如 "CreateOrder"
	Version     string   // 版本号，如 "v2"
	Endpoint    Endpoint // 选中的节点
}

// Router 路由引擎
type Router struct {
	client   *clientv3.Client
	cache    *sync.Map // serviceKey -> []Endpoint
	balancer LoadBalancer
	prefix   string
}

// New 创建路由引擎，初始化 etcd 连接并启动 watch
func New(cfg *config.EtcdConfig) (*Router, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd connect: %w", err)
	}

	prefix := strings.TrimPrefix(cfg.Prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s", prefix) // 格式：/services

	r := &Router{
		client:   cli,
		cache:    &sync.Map{},
		balancer: NewWeightedRoundRobin(),
		prefix:   prefix,
	}

	// 启动全量加载 + watch
	if err := r.bootstrap(); err != nil {
		return nil, err
	}
	go r.watch()

	return r, nil
}

// parseTarget 解析 "service/method" 格式。
// 版本信息通过独立的 RouteContext.Version 传递，不再从 target 中解析。
func parseTarget(target string) (service, method string) {
	parts := strings.Split(target, "/")
	switch len(parts) {
	case 2:
		return parts[0], parts[1], ""
	case 3:
		return parts[0], parts[1], parts[2]
	default:
		return target, "", ""
	}
}

// Route 执行路由决策：根据 target 找到对应的下游微服务节点
func (r *Router) Route(ctx context.Context, routeCtx RouteContext) (*RouteTarget, error) {
	service, method, version := parseTarget(routeCtx.Target)
	key := r.serviceKey(service, version)

	// 1. 读本地缓存
	val, ok := r.cache.Load(key)
	if !ok {
		// 2. 缓存未命中，回源 etcd
		endpoints, err := r.lookupFromEtcd(ctx, service, version)
		if err != nil {
			return nil, fmt.Errorf("lookup etcd: %w", err)
		}
		r.cache.Store(key, endpoints)
		val = endpoints
	}

	endpoints, ok := val.([]Endpoint)
	if !ok {
		return nil, errors.New("not found endpoints")
	}

	healthy := filterHealthy(endpoints)
	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy endpoint for %s", routeCtx.Target)
	}

	// 3. 负载均衡选择节点
	selected := r.balancer.Select(healthy, routeCtx)

	return &RouteTarget{
		ServiceName: service,
		MethodName:  method,
		Version:     version,
		Endpoint:    selected,
	}, nil
}

func (r *Router) serviceKey(service, version string) string {
	if version == "" {
		version = "_default"
	}
	return fmt.Sprintf("%s/%s/%s", r.prefix, service, version)
}

// parseEndpoint 解析 etcd value 中的单个 Endpoint，只保留健康节点。
// registry.Register 写入的是单个 Endpoint 的 JSON，不是数组。
func parseEndpoint(key string, value []byte) (Endpoint, bool) {
	var ep Endpoint
	if err := json.Unmarshal(value, &ep); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("unmarshal endpoint failed")
		return Endpoint{}, false
	}
	if !ep.Healthy {
		return Endpoint{}, false
	}
	return ep, true
}

// serviceVersionPrefix 返回某服务版本下的所有实例前缀：/services/{service}/{version}/
func (r *Router) serviceVersionPrefix(service, version string) string {
	if version == "" {
		version = "_default"
	}
	return fmt.Sprintf("%s/%s/%s/", r.prefix, service, version)
}

// serviceVersionFromKey 从完整注册键 /services/{service}/{version}/{instanceID}
// 中提取服务版本前缀 /services/{service}/{version}
func (r *Router) serviceVersionFromKey(key string) string {
	key = strings.TrimPrefix(key, r.prefix)
	key = strings.Trim(key, "/")
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return ""
	}
	service := parts[0]
	version := parts[1]
	if version == "_default" {
		version = ""
	}
	return r.serviceKey(service, version)
}

// 从etcd获取端点连接信息，只返回健康节点
func (r *Router) lookupFromEtcd(ctx context.Context, service, version string) ([]Endpoint, error) {
	prefix := r.serviceVersionPrefix(service, version)
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var endpoints []Endpoint
	for _, kv := range resp.Kvs {
		if ep, ok := parseEndpoint(string(kv.Key), kv.Value); ok {
			endpoints = append(endpoints, ep)
		}
	}

	return endpoints, nil
}

// refreshServiceCache 重新聚合某个服务版本下的所有健康实例并写入缓存。
func (r *Router) refreshServiceCache(serviceVersionKey string) error {
	parts := strings.Split(strings.Trim(serviceVersionKey, "/"), "/")
	if len(parts) < 2 {
		return nil
	}

	service := parts[len(parts)-2]
	version := parts[len(parts)-1]
	if service == "" {
		return nil
	}

	endpoints, err := r.lookupFromEtcd(context.Background(), service, version)
	if err != nil {
		return err
	}

	if len(endpoints) > 0 {
		r.cache.Store(serviceVersionKey, endpoints)
	} else {
		r.cache.Delete(serviceVersionKey)
	}

	return nil
}

// watch 持续监听 etcd 变更，只缓存健康节点；若某服务全部下线则剔除缓存
func (r *Router) watch() {
	watchChan := r.client.Watch(context.Background(), r.prefix, clientv3.WithPrefix())
	for resp := range watchChan {
		for _, ev := range resp.Events {
			serviceVersionKey := r.serviceVersionFromKey(string(ev.Kv.Key))
			if serviceVersionKey == "" {
				continue
			}

			switch ev.Type {
			case clientv3.EventTypePut:
				if _, ok := parseEndpoint(string(ev.Kv.Key), ev.Kv.Value); !ok {
					r.cache.Delete(serviceVersionKey)
					continue
				}
				if err := r.refreshServiceCache(serviceVersionKey); err != nil {
					log.Warn().Err(err).Str("key", serviceVersionKey).Msg("refresh service cache failed")
				}
			case clientv3.EventTypeDelete:
				if err := r.refreshServiceCache(serviceVersionKey); err != nil {
					log.Warn().Err(err).Str("key", serviceVersionKey).Msg("refresh service cache failed")
				}
			}
		}
	}
}

// bootstrap 启动时全量加载 etcd 数据到本地缓存，按服务版本聚合，只保留健康节点
func (r *Router) bootstrap() error {
	resp, err := r.client.Get(context.Background(), r.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	groups := make(map[string][]Endpoint)
	for _, kv := range resp.Kvs {
		ep, ok := parseEndpoint(string(kv.Key), kv.Value)
		if !ok {
			continue
		}

		serviceVersionKey := r.serviceVersionFromKey(string(kv.Key))
		if serviceVersionKey == "" {
			continue
		}

		groups[serviceVersionKey] = append(groups[serviceVersionKey], ep)
	}

	for key, eps := range groups {
		r.cache.Store(key, eps)
	}

	return nil
}

func filterHealthy(endpoints []Endpoint) []Endpoint {
	var result []Endpoint
	for _, ep := range endpoints {
		if ep.Healthy {
			result = append(result, ep)
		}
	}
	return result
}
```

### 4.4 `internal/router/balancer.go`

```go
package router

import (
	"sync/atomic"
)

// LoadBalancer 负载均衡接口
type LoadBalancer interface {
	Select(endpoints []Endpoint, ctx RouteContext) Endpoint
}

// WeightedRoundRobin 加权轮询负载均衡器
type WeightedRoundRobin struct {
	counter uint64
}

// NewWeightedRoundRobin 创建加权轮询器
func NewWeightedRoundRobin() *WeightedRoundRobin {
	return &WeightedRoundRobin{}
}

// Select 按权重选择下游节点
func (w *WeightedRoundRobin) Select(endpoints []Endpoint, ctx RouteContext) Endpoint {
	totalWeight := uint32(0)
	for _, ep := range endpoints {
		totalWeight += ep.Weight
	}

	if totalWeight == 0 {
		return endpoints[0]
	}

	count := atomic.AddUint64(&w.counter, 1)
	pos := uint32((count - 1) % uint64(totalWeight))

	var current uint32
	for _, ep := range endpoints {
		current += ep.Weight
		if pos < current {
			return ep
		}
	}

	return endpoints[0]
}
```

### 4.5 `internal/protocol/protocol.go`

```go
package protocol

import (
	"context"
	"time"

	"github.com/daheige/bridge-svc/internal/router"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
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
func Factory(protocol router.ProtocolType) Handler {
	switch protocol {
	case router.ProtocolGRPC:
		return NewGRPCHandler() // 下游是 gRPC 服务
	case router.ProtocolHTTP:
		return NewHTTPHandler() // 下游是 HTTP 服务
	default:
		return nil
	}
}
```

### 4.6 `internal/protocol/grpc.go`

```go
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
```

### 4.7 `internal/protocol/http.go`

```go
package protocol

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/daheige/bridge-svc/internal/router"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
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
```

### 4.8 `internal/pool/grpcpool.go`

```go
package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCPool gRPC 连接池
// 管理 Bridge 到下游 gRPC 微服务的连接，实现连接复用
type GRPCPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn // key: 下游地址, value: gRPC 连接
}

// NewGRPCPool 创建连接池
func NewGRPCPool() *GRPCPool {
	return &GRPCPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Get 获取或创建到下游 gRPC 服务的连接
func (p *GRPCPool) Get(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	// 清理地址中的 http:// 或 https:// 前缀（gRPC 不需要）
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")

	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()

	if ok && p.isHealthy(conn) {
		return conn, nil
	}

	// 创建新连接
	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if conn, ok := p.conns[addr]; ok && p.isHealthy(conn) {
		return conn, nil
	}

	newConn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16<<20), // 16MB
			grpc.MaxCallSendMsgSize(16<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	p.conns[addr] = newConn
	return newConn, nil
}

// Put 归还连接（gRPC 连接长期复用，无需真正归还）
func (p *GRPCPool) Put(conn *grpc.ClientConn) {
	// gRPC 连接长期复用，这里仅做健康检查占位
}

func (p *GRPCPool) isHealthy(conn *grpc.ClientConn) bool {
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close 关闭所有连接
func (p *GRPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		conn.Close()
	}
}
```

### 4.9 `internal/resilience/circuit.go`

```go
package resilience

import (
	"errors"
	"sync"

	"github.com/sony/gobreaker/v2"
	"github.com/daheige/bridge-svc/internal/config"
)

// ErrCircuitOpen 熔断器打开错误
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreakerManager 熔断器管理器
// 按下游微服务 endpoint 维度管理熔断器，保护 Bridge 不被故障下游拖垮
// 使用 gobreaker/v2 的泛型类型 CircuitBreaker[T any]
type CircuitBreakerManager struct {
	breakers sync.Map // endpoint -> *gobreaker.CircuitBreaker[interface{}]
	cfg      config.CircuitBreakerConfig
}

// NewCircuitBreakerManager 创建熔断器管理器
func NewCircuitBreakerManager(cfg config.CircuitBreakerConfig) *CircuitBreakerManager {
	return &CircuitBreakerManager{cfg: cfg}
}

// Get 获取或创建指定下游 endpoint 的熔断器
// gobreaker/v2 使用泛型类型 CircuitBreaker[T any]
func (m *CircuitBreakerManager) Get(endpoint string) *gobreaker.CircuitBreaker[interface{}] {
	if cb, ok := m.breakers.Load(endpoint); ok {
		return cb.(*gobreaker.CircuitBreaker[interface{}])
	}

	settings := gobreaker.Settings{
		Name:        endpoint,
		MaxRequests: m.cfg.HalfOpenMax,
		Interval:    m.cfg.Interval,
		Timeout:     m.cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= m.cfg.FailureRatio
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			// TODO: 记录状态变化到日志/指标
		},
	}

	cb := gobreaker.NewCircuitBreaker[interface{}](settings)
	actual, _ := m.breakers.LoadOrStore(endpoint, cb)
	return actual.(*gobreaker.CircuitBreaker[interface{}])
}

// Execute 在熔断器保护下执行函数
// 如果熔断器打开，直接返回 ErrCircuitOpen，避免请求故障下游
func (m *CircuitBreakerManager) Execute(endpoint string, fn func() (interface{}, error)) (interface{}, error) {
	cb := m.Get(endpoint)
	result, err := cb.Execute(fn)
	if err == gobreaker.ErrOpenState {
		return nil, ErrCircuitOpen
	}
	return result, err
}
```

### 4.10 `internal/resilience/retry.go`

```go
package resilience

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryableFunc 可重试函数签名
type RetryableFunc func() error

// RetryConfig 重试配置
type RetryConfig struct {
	MaxAttempts       uint32
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	RetryableCodes    []codes.Code
}

// DoRetry 执行带重试的函数
// 对下游调用失败进行重试，支持指数退避
func DoRetry(ctx context.Context, cfg RetryConfig, fn RetryableFunc) error {
	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := uint32(0); attempt < cfg.MaxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err

			// 检查是否可重试
			if !isRetryable(err, cfg.RetryableCodes) {
				return err
			}

			// 最后一次尝试，不再等待
			if attempt == cfg.MaxAttempts-1 {
				break
			}

			// 等待后重试
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = minDuration(
					time.Duration(float64(backoff)*cfg.BackoffMultiplier),
					cfg.MaxBackoff,
				)
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

func isRetryable(err error, retryableCodes []codes.Code) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, code := range retryableCodes {
		if st.Code() == code {
			return true
		}
	}
	return false
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
```

### 4.11 `internal/resilience/ratelimit.go`

```go
package resilience

import (
	"context"
	"errors"

	"golang.org/x/time/rate"
)

// ErrRateLimited 限流错误
var ErrRateLimited = errors.New("rate limited")

// RateLimiter 令牌桶限流器
// 对业务方请求进行限流，保护下游微服务不被突发流量打垮
type RateLimiter struct {
	limiter *rate.Limiter
}

// NewRateLimiter 创建限流器
func NewRateLimiter(rps int, burstSize int) *RateLimiter {
	return &RateLimiter{
		limiter: rate.NewLimiter(rate.Limit(rps), burstSize),
	}
}

// Allow 检查是否允许通过
func (r *RateLimiter) Allow() bool {
	return r.limiter.Allow()
}

// Wait 等待获取令牌
func (r *RateLimiter) Wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}
```

### 4.12 `internal/middleware/chain.go`

```go
package middleware

import (
	"context"

	"google.golang.org/grpc"
)

// ChainUnaryServer 组装一元拦截器链
// 将多个拦截器按顺序组合，形成处理链
func ChainUnaryServer(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	n := len(interceptors)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		chain := handler
		for i := n - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chain
			chain = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				return interceptor(currentCtx, currentReq, info, func(nestedCtx context.Context, nestedReq interface{}) (interface{}, error) {
					return next(nestedCtx, nestedReq)
				})
			}
		}
		return chain(ctx, req)
	}
}
```

### 4.13 `internal/middleware/recovery.go`

```go
package middleware

import (
	"context"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryInterceptor panic 恢复拦截器
// 捕获业务处理中的 panic，防止单个请求拖垮整个 Bridge 进程
func RecoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Str("method", info.FullMethod).
				Interface("panic", r).
				Msg("panic recovered")
			err = status.Errorf(codes.Internal, "internal error: %v", r)
		}
	}()
	return handler(ctx, req)
}
```

### 4.14 `internal/middleware/logging.go`

```go
package middleware

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// LoggingInterceptor 结构化日志拦截器
// 记录每个业务方请求的详细信息，便于排查问题
func LoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	md, _ := metadata.FromIncomingContext(ctx)

	resp, err := handler(ctx, req)

	log.Info().
		Str("method", info.FullMethod).
		Dur("duration", time.Since(start)).
		Interface("metadata", md).
		Err(err).
		Msg("unary call")

	return resp, err
}
```

### 4.15 `internal/middleware/auth.go`

```go
package middleware

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthInterceptor 认证拦截器（占位实现）
// 验证业务方请求的合法性，可集成 Casbin 或 JWT
func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	authHeader := md.Get("authorization")
	if len(authHeader) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing authorization header")
	}

	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	if token == "" {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token format")
	}

	// TODO: 集成 Casbin 或 JWT 验证
	// if err := validateToken(token); err != nil { ... }

	return handler(ctx, req)
}
```

### 4.16 `internal/observability/tracing.go`

```go
package observability

import (
	"context"
	"fmt"

	"github.com/daheige/bridge-svc/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

var tracer trace.Tracer

// InitTracer 初始化 OpenTelemetry Trace
// 将 Trace 数据通过 OTLP 发送到 OpenObserve
func InitTracer(cfg config.ObservabilityConfig) error {
	if cfg.TraceEndpoint == "" {
		tracer = otel.Tracer("bridge-svc")
		return nil
	}

	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(cfg.TraceEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("create trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
		)),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer = provider.Tracer("bridge-svc")

	return nil
}

// ExtractTraceContext 从 gRPC metadata 提取 trace context
// 从业务方请求中提取 trace 信息，实现链路追踪透传
func ExtractTraceContext(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	carrier := propagation.MapCarrier{}
	for k, v := range md {
		if len(v) > 0 {
			carrier[k] = v[0]
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// StartSpan 启动新 span
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}
```

### 4.17 `internal/observability/logging.go`

```go
package observability

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogger 初始化 zerolog
// 零分配 JSON 日志，便于采集到 OpenObserve
func InitLogger(level string) {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	l, err := zerolog.ParseLevel(level)
	if err != nil {
		l = zerolog.InfoLevel
	}

	log.Logger = zerolog.New(os.Stdout).
		Level(l).
		With().
		Timestamp().
		Caller().
		Logger()
}
```

### 4.18 `internal/observability/metrics.go`

```go
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestTotal 请求总数
	// 按方法和状态码统计业务方请求
	RequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_request_total",
		Help: "Total number of requests from upstream",
	}, []string{"method", "status"})

	// RequestDuration 请求耗时
	// 按方法统计从业务方请求到响应的总耗时
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bridge_request_duration_seconds",
		Help:    "Request duration in seconds (upstream to downstream)",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	// ActiveConnections 活跃连接数
	// 当前与业务方建立的 gRPC 连接数
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_active_connections",
		Help: "Number of active connections from upstream",
	})
)
```

### 4.19 `internal/observability/init.go`

```go
package observability

import (
	"fmt"

	"github.com/daheige/bridge-svc/internal/config"
)

// Init 初始化所有可观测性组件
func Init(cfg config.ObservabilityConfig) error {
	InitLogger(cfg.LogLevel)

	if err := InitTracer(cfg); err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}

	return nil
}
```

### 4.20 `pkg/utils/any.go`

```go
package utils

import (
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/proto"
)

// MarshalToAny 将 protobuf message 打包为 Any
// 业务方使用此函数将业务请求打包为 Any 传入 Bridge
func MarshalToAny(msg proto.Message) (*anypb.Any, error) {
	any, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal to any: %w", err)
	}
	return any, nil
}

// UnmarshalFromAny 从 Any 解包为指定类型
// 业务方使用此函数从 Bridge 响应中解包业务响应
func UnmarshalFromAny(any *anypb.Any, dst proto.Message) error {
	if err := anypb.UnmarshalTo(any, dst, proto.UnmarshalOptions{}); err != nil {
		return fmt.Errorf("unmarshal from any: %w", err)
	}
	return nil
}
```

### 4.21 `pkg/registry/discovery.go`

`Discovery` 接口提供基于 etcd 的服务发现能力，支持一次性 `Get` 和持续 `Watch`。

```go
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

// Discovery 服务发现接口。
type Discovery interface {
	Get(ctx context.Context, service, version string) ([]Endpoint, error)
	Watch(ctx context.Context, service, version string) (Watcher, error)
}

// Watcher 服务发现监听器。
type Watcher interface {
	Next() ([]Endpoint, error)
	Stop()
}

// etcdDiscovery 基于 etcd 的服务发现实现。
type etcdDiscovery struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
}

// NewEtcdDiscovery 创建 etcd 服务发现器。
func NewEtcdDiscovery(etcdEndpoints []string, prefix string, timeout time.Duration) (Discovery, error) {
	if timeout <= 0 {
		timeout = DefaultEtcdTimeout
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		prefix = "/services"
	}
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s", prefix)

	return &etcdDiscovery{
		client:  cli,
		prefix:  prefix,
		timeout: timeout,
	}, nil
}

func (d *etcdDiscovery) Get(ctx context.Context, service, version string) ([]Endpoint, error) {
	prefix := d.servicePrefix(service, version)

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	resp, err := d.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get failed: %w", err)
	}

	return parseEndpoints(resp.Kvs), nil
}

func (d *etcdDiscovery) Watch(ctx context.Context, service, version string) (Watcher, error) {
	prefix := d.servicePrefix(service, version)

	w := &etcdWatcher{
		ch:     make(chan []Endpoint, 1),
		stopCh: make(chan struct{}),
	}

	endpoints, err := d.Get(ctx, service, version)
	if err != nil {
		return nil, err
	}
	w.ch <- endpoints

	watchCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go func() {
		defer close(w.ch)
		watchCh := d.client.Watch(watchCtx, prefix, clientv3.WithPrefix())
		for {
			select {
			case <-w.stopCh:
				return
			case resp, ok := <-watchCh:
				if !ok {
					return
				}
				if resp.Err() != nil {
					continue
				}
				endpoints, err := d.Get(watchCtx, service, version)
				if err != nil {
					continue
				}
				if !w.push(endpoints) {
					return
				}
			}
		}
	}()

	return w, nil
}

func (d *etcdDiscovery) servicePrefix(service, version string) string {
	if version == "" {
		version = "_default"
	}
	return fmt.Sprintf("%s/%s/%s/", d.prefix, service, version)
}

func parseEndpoints(kvs []*mvccpb.KeyValue) []Endpoint {
	seen := make(map[string]struct{})
	var endpoints []Endpoint
	for _, kv := range kvs {
		var ep Endpoint
		if err := json.Unmarshal(kv.Value, &ep); err != nil {
			continue
		}
		if ep.Address == "" {
			continue
		}
		if _, ok := seen[ep.Address]; ok {
			continue
		}
		seen[ep.Address] = struct{}{}
		endpoints = append(endpoints, ep)
	}
	return endpoints
}
```

### 4.22 `pkg/registry/resolver.go`

基于 `Discovery` 实现自定义 gRPC resolver，业务方可以通过 `etcd:///` scheme 直接访问 etcd 发现的服务。

```go
package registry

import (
	"context"
	"log"
	"strings"
	"sync"

	"google.golang.org/grpc/resolver"
)

const defaultScheme = "etcd"

// NewEtcdResolverBuilder 创建 gRPC resolver builder。
func NewEtcdResolverBuilder(discovery Discovery, scheme string) resolver.Builder {
	if scheme == "" {
		scheme = defaultScheme
	}
	return &etcdResolverBuilder{
		discovery: discovery,
		scheme:    scheme,
	}
}

// RegisterEtcdResolver 使用指定 Discovery 注册 etcd gRPC resolver。
func RegisterEtcdResolver(discovery Discovery, scheme string) {
	resolver.Register(NewEtcdResolverBuilder(discovery, scheme))
}
```

使用示例：

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
```

---

## 5. 服务入口与启动

### 5.1 `internal/server/server.go`

```go
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	bridgev1 "github.com/daheige/bridge-svc/api/v1"
	"github.com/daheige/bridge-svc/internal/config"
	"github.com/daheige/bridge-svc/internal/middleware"
	"github.com/daheige/bridge-svc/internal/observability"
	"github.com/daheige/bridge-svc/internal/protocol"
	"github.com/daheige/bridge-svc/internal/resilience"
	"github.com/daheige/bridge-svc/internal/router"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// BridgeServer Bridge 服务实现
// 对外暴露 gRPC 接口，接收业务方请求，转发到下游微服务
type BridgeServer struct {
	bridgev1.UnimplementedBridgeServiceServer

	router     *router.Router                 // 路由引擎
	breakerMgr *resilience.CircuitBreakerManager // 熔断器管理
	cfg        *config.Config                 // 配置
}

// New 创建 BridgeServer 实例
func New(cfg *config.Config) (*BridgeServer, error) {
	r, err := router.New(&cfg.Etcd)
	if err != nil {
		return nil, fmt.Errorf("init router: %w", err)
	}

	return &BridgeServer{
		router:     r,
		breakerMgr: resilience.NewCircuitBreakerManager(cfg.Resilience.CircuitBreaker),
		cfg:        cfg,
	}, nil
}

// CallUnary 一元调用处理
// 接收业务方的 gRPC 请求，路由到下游微服务并返回响应
func (s *BridgeServer) CallUnary(ctx context.Context, req *bridgev1.UnaryRequest) (*bridgev1.UnaryResponse, error) {
	start := time.Now()

	// 1. 路由决策：根据 target 找到下游微服务节点
	routeCtx := router.RouteContext{
		Target:   req.Target,
		Protocol: router.ProtocolType(req.Protocol),
		Metadata: toMetadataMD(req.Metadata),
	}

	target, err := s.router.Route(ctx, routeCtx)
	if err != nil {
		return &bridgev1.UnaryResponse{
			Status: status.New(codes.Unavailable, fmt.Sprintf("routing failed: %v", err)).Proto(),
		}, nil
	}

	// 2. 计算超时：业务方指定 > 配置默认值
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = s.cfg.Protocols.DefaultTimeout
	}

	// 3. 独立超时上下文（避免上游 deadline 干扰熔断器判断）
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 4. 获取协议处理器（根据下游微服务协议类型）
	handler := protocol.Factory(target.Endpoint.Protocol)
	if handler == nil {
		return &bridgev1.UnaryResponse{
			Status: status.New(codes.InvalidArgument, fmt.Sprintf("unsupported protocol: %s", req.Protocol)).Proto(),
			UpstreamNode: target.Endpoint.Address,
		}, nil
	}

	// 5. 熔断保护执行：在熔断器保护下调用下游微服务
	result, err := s.breakerMgr.Execute(target.Endpoint.Address, func() (interface{}, error) {
		return handler.Call(callCtx, target, req.Payload, toMetadataMD(req.Metadata), timeout)
	})

	latency := uint64(time.Since(start).Microseconds())

	if err != nil {
		statusCode := codes.Unavailable
		if errors.Is(err, resilience.ErrCircuitOpen) {
			statusCode = codes.Unavailable
		}

		return &bridgev1.UnaryResponse{
			Status: status.New(statusCode, err.Error()).Proto(),
			UpstreamNode: target.Endpoint.Address,
			LatencyUs:    latency,
		}, nil
	}

	resp := result.(*protocol.Response)
	return &bridgev1.UnaryResponse{
		Payload:      resp.Payload,
		Metadata:     mdToMap(resp.Metadata),
		UpstreamNode: target.Endpoint.Address,
		LatencyUs:    latency,
	}, nil
}

// toMetadataMD 将 map[string]string 转换为 metadata.MD
func toMetadataMD(m map[string]string) metadata.MD {
	md := metadata.MD{}
	for k, v := range m {
		md[k] = []string{v}
	}

	return md
}

// mdToMap 转换 metadata.MD 为普通 map
func mdToMap(md metadata.MD) map[string]string {
	m := make(map[string]string, len(md))
	for k, v := range md {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	return m
}

// Health 健康检查
func (s *BridgeServer) Health(_ context.Context, req *bridgev1.HealthRequest) (*bridgev1.HealthResponse, error) {
	return &bridgev1.HealthResponse{
		Status: bridgev1.HealthResponse_SERVING,
	}, nil
}

// Start 启动 Bridge 服务
// 1. 初始化可观测性（日志、Trace、指标）
// 2. 创建 gRPC Server，注册 BridgeService
// 3. 启动 Prometheus 指标 HTTP 服务
// 4. 监听 gRPC 端口，接收业务方请求
func Start(cfg *config.Config) error {
	// 初始化可观测性
	if err := observability.Init(cfg.Observability); err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	// 创建 Bridge 服务实例
	bridge, err := New(cfg)
	if err != nil {
		return err
	}

	// 组装拦截器链（从外到内：Recovery -> Logging -> Auth -> RateLimit -> Trace）
	chain := middleware.ChainUnaryServer(
		middleware.RecoveryInterceptor,
		middleware.LoggingInterceptor,
		// middleware.AuthInterceptor,
		// middleware.RateLimitInterceptor,
	)

	// 创建 gRPC server（业务方通过此 Server 调用 Bridge）
	gs := grpc.NewServer(
		grpc.MaxConcurrentStreams(cfg.Server.MaxConcurrentStreams),
		grpc.UnaryInterceptor(chain),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     cfg.Server.KeepaliveTime,
			MaxConnectionAgeGrace: cfg.Server.KeepaliveTimeout,
		}),
	)

	bridgev1.RegisterBridgeServiceServer(gs, bridge)
	reflection.Register(gs) // 启用反射，便于 grpcurl 调试

	// 启动 metrics HTTP server（Prometheus 采集指标）
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		addr := fmt.Sprintf(":%d", cfg.Observability.MetricsPort)
		log.Info().Str("addr", addr).Msg("metrics server starting")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Error().Err(err).Msg("metrics server failed")
		}
	}()

	// 监听 gRPC 端口（业务方通过此端口调用 Bridge）
	lis, err := net.Listen("tcp", cfg.Server.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	log.Info().Str("addr", cfg.Server.ListenAddr).Msg("bridge gRPC server starting, waiting for upstream requests")
	return gs.Serve(lis)
}
```

### 5.2 `cmd/bridge/main.go`

```go
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/daheige/bridge-svc/internal/config"
	"github.com/daheige/bridge-svc/internal/server"
)

func main() {
	cfg, err := config.Load("config/bridge.yaml")
	if err != nil {
		log.Fatal().Err(err).Msg("load config failed")
	}

	// 优雅关闭：监听系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(cfg)
	}()

	select {
	case err := <-errCh:
		log.Fatal().Err(err).Msg("server error")
	case sig := <-sigCh:
		log.Info().Str("signal", sig.String()).Msg("shutdown signal received, graceful shutdown")
	}
}
```

---

## 6. 配置文件

### 6.1 `config/bridge.yaml`

```yaml
# Bridge 服务配置
# 业务方通过 gRPC 调用 Bridge，Bridge 转发到下游微服务

server:
  listen_addr: "0.0.0.0:50052"       # gRPC 监听地址（业务方调用此端口）
  max_concurrent_streams: 10000       # 最大并发流
  keepalive_time: 7200s              # 连接保活时间
  keepalive_timeout: 20s             # 保活超时

# etcd配置
etcd:
  endpoints:                         # etcd 集群地址（服务发现）
    - "http://127.0.0.1:12379"
  dial_timeout: 5s                   # 连接超时
  prefix: "/services/"               # 服务注册前缀

# 协议配置
protocols:
  default_timeout: 30s             # Bridge 到下游的默认超时
  grpc:
    max_send_msg_size: 16777216    # 16MB（下游 gRPC 发送限制）
    max_recv_msg_size: 16777216    # 16MB（下游 gRPC 接收限制）
    dial_timeout: 10s               # 连接下游 gRPC 超时
  http:
    max_conns_per_host: 100        # 每下游主机最大连接数
    read_timeout: 10s              # 下游 HTTP 读超时
    write_timeout: 10s             # 下游 HTTP 写超时

# 限流熔断
resilience:
  circuit_breaker:
    max_requests: 3                # 半开状态最大请求数
    interval: 10s                  # 统计周期
    timeout: 30s                   # 熔断持续时间
    failure_ratio: 0.6             # 触发熔断的失败率阈值
    success_ratio: 0.5             # 恢复熔断的成功率阈值
    half_open_max: 1               # 半开状态最大探测请求
  rate_limiter:
    rps: 10000                     # 每秒请求数（限流）
    burst_size: 500                # 突发容量
  retry:
    max_attempts: 3                # 最大重试次数
    initial_backoff: 100ms         # 初始退避时间
    max_backoff: 5s                # 最大退避时间
    backoff_multiplier: 2.0        # 退避乘数

# 可观测性
observability:
  log_level: "info"                # 日志级别
  trace_endpoint: "localhost:5081"  # OTLP Trace 接收端
  metrics_port: 9090               # Prometheus 指标端口
  service_name: "bridge-svc"   # 服务名
  service_version: "v0.1.0"        # 服务版本
```

---

## 7. Makefile 与构建

### 7.1 `Makefile`

```makefile
.PHONY: all build proto test clean docker

BINARY_NAME=bridge-svc
DOCKER_IMAGE=your-registry/bridge-svc
VERSION=$(shell git describe --tags --always --dirty)

all: proto build

# 生成 protobuf 代码
proto:
	@echo "Generating protobuf code..."
	@protoc -I api/v1 \
           --go_out=api/v1 --go_opt=paths=source_relative \
           --go-grpc_out=api/v1 --go-grpc_opt=paths=source_relative \
           api/v1/bridge.proto
	@echo "gen code success"

# 构建二进制
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -ldflags "-X main.version=$(VERSION)" -o bin/$(BINARY_NAME) ./cmd/bridge

# 运行测试
test:
	@go test -v -race ./...

# 清理
clean:
	@rm -rf bin/
	@rm -f api/v1/*.pb.go

# 构建 Docker 镜像（基于 Go 1.24）
docker:
	@docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

# 本地运行
run: build
	@./bin/$(BINARY_NAME)

# 格式化代码
fmt:
	@gofmt -w -s .
	@goimports -w .

# 静态检查
lint:
	@golangci-lint run ./...

# 依赖更新（升级到最新版本）
deps:
	@go get -u ./...
	@go mod tidy
	@go mod verify
```

### 7.2 `Dockerfile`（基于 Go 1.25）

```dockerfile
# 构建阶段（Go 1.25 Alpine）
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags "-w -s" -o bridge-svc ./cmd/bridge

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /app/

COPY --from=builder /app/bridge-svc /app/bridge-svc
COPY config/bridge.yaml ./config/

EXPOSE 50051 9090

ENTRYPOINT ["/app/bridge-svc"]
```

---

## 8. 部署方案

### 8.1 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bridge-svc
  namespace: infrastructure
spec:
  replicas: 3
  selector:
    matchLabels:
      app: bridge-svc
  template:
    metadata:
      labels:
        app: bridge-svc
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: bridge
          image: your-registry/bridge-svc:v0.1.0
          ports:
            - containerPort: 50051
              name: grpc        # 业务方通过此端口调用 Bridge（gRPC）
            - containerPort: 9090
              name: metrics     # Prometheus 指标端口
          env:
            - name: BRIDGE_SERVER_LISTEN_ADDR
              value: "0.0.0.0:50051"
            - name: BRIDGE_ETCD_ENDPOINTS
              value: "etcd-0:2379,etcd-1:2379,etcd-2:2379"
            - name: BRIDGE_OBSERVABILITY_TRACE_ENDPOINT
              value: "otel-collector.monitoring:4317"
            - name: GODEBUG
              value: "madvdontneed=0"
          resources:
            requests:
              memory: "512Mi"
              cpu: "500m"
            limits:
              memory: "2Gi"
              cpu: "2000m"
          livenessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: config
              mountPath: /root/config
      volumes:
        - name: config
          configMap:
            name: bridge-config
---
apiVersion: v1
kind: Service
metadata:
  name: bridge-svc
  namespace: infrastructure
spec:
  selector:
    app: bridge-svc
  ports:
    - port: 50051
      targetPort: 50051
      name: grpc        # 业务方调用 Bridge 的 gRPC 端口
    - port: 9090
      targetPort: 9090
      name: metrics     # Prometheus 指标端口
  type: ClusterIP
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: bridge-config
  namespace: infrastructure
data:
  bridge.yaml: |
    server:
      listen_addr: "0.0.0.0:50051"
      max_concurrent_streams: 1000
    etcd:
      endpoints:
        - "etcd-0:2379"
        - "etcd-1:2379"
        - "etcd-2:2379"
      prefix: "/services/"
    # ... 其他配置
```

---

## 9. 调用方式与接入指南

### 9.1 业务方调用 Bridge（Go gRPC 客户端示例）

参考项目中的 [client/main.go](client/main.go)：

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
		// 如果使用k8s命名服务以及headless方式访问，可启用 round_robin 负载均衡
		// grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(30*time.Minute),
		grpc.WithMaxCallAttempts(3),
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
		// target: 下游服务名/方法名/版本
		// 对应 gRPC 方法路径，如 /Hello.Greeter/SayHello
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
```

### 9.2 业务方调用 Bridge（Java gRPC 客户端示例）

```java
// 使用 protobuf-java 和 grpc-java
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("bridge-svc", 50052)
    .usePlaintext()
    .build();

BridgeServiceGrpc.BridgeServiceBlockingStub stub = 
    BridgeServiceGrpc.newBlockingStub(channel);

// 构建业务请求
Any payload = Any.pack(CreateOrderRequest.newBuilder()
    .setUserId("user-123")
    .setProductId("product-456")
    .setQuantity(2)
    .build());

// 调用 Bridge
UnaryResponse response = stub.callUnary(UnaryRequest.newBuilder()
    .setTarget("order_service/CreateOrder/v1")
    .setProtocol("GRPC")
    .setPayload(payload)
    .putMetadata("authorization", "Bearer token123")
    .setTimeoutMs(3000)
    .build());

// 处理响应
if (response.getStatus().getCode() != 0) {
    System.out.println("Error: " + response.getStatus().getMessage());
} else {
    CreateOrderResponse orderResp = response.getPayload().unpack(CreateOrderResponse.class);
    System.out.println("Order created: " + orderResp.getOrderId());
}
```

### 9.3 业务方调用 Bridge（Python gRPC 客户端示例）

```python
import grpc
from bridge.v1 import bridge_pb2, bridge_pb2_grpc
from google.protobuf import any_pb2

# 建立连接
channel = grpc.insecure_channel('bridge-svc:50052')
stub = bridge_pb2_grpc.BridgeServiceStub(channel)

# 构建业务请求
from order_service import create_order_pb2
order_req = create_order_pb2.CreateOrderRequest(
    user_id="user-123",
    product_id="product-456",
    quantity=2
)

# 打包为 Any
payload = any_pb2.Any()
payload.Pack(order_req)

# 调用 Bridge
response = stub.CallUnary(bridge_pb2.UnaryRequest(
    target="order_service/CreateOrder/v1",
    protocol="GRPC",
    payload=payload,
    metadata={
        "authorization": "Bearer token123",
        "x-request-id": "req-456"
    },
    timeout_ms=3000
))

# 处理响应
if response.status.code != 0:
    print(f"Error: {response.status.message}")
else:
    order_resp = create_order_pb2.CreateOrderResponse()
    response.payload.Unpack(order_resp)
    print(f"Order created: {order_resp.order_id}")
```

### 9.4 下游微服务注册到 etcd

下游微服务启动时需要将自身信息注册到 etcd，Bridge 通过 etcd 发现服务并进行路由。

#### 9.4.1 注册数据结构

`pkg/registry/registry.go` 实现基于 etcd 租约的服务注册，支持 KeepAlive 断线后自动重新注册。

```go
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// DefaultTTL etcd 租约默认 TTL，单位秒。
const DefaultTTL = 10

// DefaultEtcdTimeout etcd 操作默认超时。
const DefaultEtcdTimeout = 5 * time.Second

// Endpoint 服务节点信息。
// 注册到 etcd 的数据结构，Bridge 据此进行路由和协议选择。
type Endpoint struct {
	Address  string            `json:"address"`  // 服务地址，如 "10.0.1.5:50051"
	Weight   uint32            `json:"weight"`   // 权重，用于负载均衡（默认 100）
	Protocol string            `json:"protocol"` // 协议类型：GRPC / HTTP（Bridge 根据此选择 Handler）
	Region   string            `json:"region"`   // 区域，如 "cn-north-1"
	Tags     map[string]string `json:"tags"`     // 标签，如 {"version": "v1", "env": "prod"}
	Healthy  bool              `json:"healthy"`  // 健康状态
}

// RegistryOption 服务注册器配置选项。
type RegistryOption func(*ServiceRegistry)

// WithTTL 设置 etcd 租约 TTL（秒）。
func WithTTL(ttl int64) RegistryOption {
	return func(r *ServiceRegistry) {
		if ttl > 0 {
			r.ttl = ttl
		}
	}
}

// WithEtcdTimeout 设置 etcd 操作超时。
func WithEtcdTimeout(timeout time.Duration) RegistryOption {
	return func(r *ServiceRegistry) {
		if timeout > 0 {
			r.etcdTimeout = timeout
		}
	}
}

// WithInstanceID 设置实例标识，用于构造唯一注册 key。
// 默认使用 sanitized 后的 endpoint.Address。
func WithInstanceID(id string) RegistryOption {
	return func(r *ServiceRegistry) {
		if id != "" {
			r.instanceID = sanitizeInstanceID(id)
		}
	}
}

// ServiceRegistry 服务注册器。
type ServiceRegistry struct {
	client      *clientv3.Client
	prefix      string
	service     string
	version     string
	instanceID  string
	endpoint    Endpoint
	leaseID     clientv3.LeaseID
	mu          sync.Mutex
	ttl         int64
	etcdTimeout time.Duration
	stop        chan struct{}
	once        sync.Once
}

// NewServiceRegistry 创建服务注册器。
// prefix: etcd 前缀，如 "/services/"
// service: 服务名，如 "order_service"
// version: 版本，如 "v2"；为空则规范化为 "_default"
func NewServiceRegistry(etcdEndpoints []string, prefix, service, version string, endpoint Endpoint, opts ...RegistryOption) (*ServiceRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		prefix = "/services/"
	}
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = fmt.Sprintf("/%s", prefix)

	if version == "" {
		version = "_default"
	}

	r := &ServiceRegistry{
		client:      cli,
		prefix:      prefix,
		service:     service,
		version:     version,
		instanceID:  sanitizeInstanceID(endpoint.Address),
		endpoint:    endpoint,
		ttl:         DefaultTTL,
		etcdTimeout: DefaultEtcdTimeout,
		stop:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// Register 注册服务到 etcd。
// 使用租约（lease）实现自动过期，服务异常退出时自动从 etcd 移除。
func (r *ServiceRegistry) Register() error {
	if err := r.registerOnce(); err != nil {
		return err
	}

	go r.keepAlive()
	return nil
}

func (r *ServiceRegistry) registerOnce() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	resp, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("grant lease failed: %w", err)
	}

	data, err := json.Marshal(r.endpoint)
	if err != nil {
		return fmt.Errorf("marshal endpoint failed: %w", err)
	}

	key := r.registryKey()
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("put endpoint failed: %w", err)
	}

	r.mu.Lock()
	r.leaseID = resp.ID
	r.mu.Unlock()

	log.Printf("registered service: %s leaseID:%v", key, resp.ID)
	return nil
}

// etcd key格式：/services/Hello.Greeter/v1/127.0.0.1-50051
func (r *ServiceRegistry) registryKey() string {
	return fmt.Sprintf("%s/%s/%s/%s", r.prefix, r.service, r.version, r.instanceID)
}

// keepAlive 保持租约，并在 KeepAlive channel 关闭后自动重新注册。
func (r *ServiceRegistry) keepAlive() {
	for {
		select {
		case <-r.stop:
			return
		default:
		}

		if !r.runKeepAlive() {
			return
		}

		// KeepAlive 异常退出，等待一小段时间后由外层循环重新建立。
		time.Sleep(time.Second)
	}
}

// runKeepAlive 对当前 leaseID 执行一次 KeepAlive。
func (r *ServiceRegistry) runKeepAlive() bool {
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()

	if leaseID == 0 {
		if err := r.reRegister(); err != nil {
			log.Printf("re-register failed (lease is zero): %v", err)
		}
		return true
	}

	ch, err := r.client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		log.Printf("keepalive init failed: %v", err)
		if err := r.reRegister(); err != nil {
			log.Printf("re-register failed: %v", err)
		}
		return true
	}

	for {
		select {
		case <-r.stop:
			return false
		case ka, ok := <-ch:
			if !ok || ka == nil {
				log.Println("keepalive channel closed, re-registering")
				if err := r.reRegister(); err != nil {
					log.Printf("re-register failed: %v", err)
				}
				return true
			}
		}
	}
}

// reRegister 在 KeepAlive 失败或 lease 失效时重新创建 lease 并写入注册信息。
func (r *ServiceRegistry) reRegister() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stop:
		return nil
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	resp, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("grant lease failed: %w", err)
	}

	data, err := json.Marshal(r.endpoint)
	if err != nil {
		return fmt.Errorf("marshal endpoint failed: %w", err)
	}

	key := r.registryKey()
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("put endpoint failed: %w", err)
	}

	r.leaseID = resp.ID
	log.Printf("re-registered service: %s leaseID:%v", key, resp.ID)
	return nil
}

// Deregister 从 etcd 注销服务，幂等且安全。
func (r *ServiceRegistry) Deregister() error {
	r.once.Do(func() {
		close(r.stop)
	})

	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.etcdTimeout)
	defer cancel()

	if leaseID != 0 {
		if _, err := r.client.Revoke(ctx, leaseID); err != nil {
			log.Printf("revoke lease failed: %v", err)
		}
		r.mu.Lock()
		r.leaseID = 0
		r.mu.Unlock()
	}

	key := r.registryKey()
	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("delete endpoint failed: %w", err)
	}

	log.Printf("deregistered service: %s", key)
	return nil
}

// sanitizeInstanceID 把地址等字符串转换为可用作 etcd key 的实例标识。
func sanitizeInstanceID(s string) string {
	s = strings.ReplaceAll(s, "://", "-")
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, ":", "-")
	s = strings.TrimSpace(s)
	if s == "" {
		return "default"
	}
	return s
}
```

### 9.4.2 gRPC 下游服务启动示例

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

	"github.com/daheige/registry"
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
		"v1",                   // 版本
		registry.Endpoint{
			Address:  "10.0.1.5:50051",  // 本机地址
			Weight:   100,
			Protocol: "GRPC",             // 协议类型
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

### 9.4.3 HTTP 下游服务启动示例

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

	"github.com/daheige/registry"
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
		"v1",                   // 版本
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

### 9.4.4 业务方调用示例（对应上述服务）

```go
// 调用 Greeter 服务（gRPC 下游）
// 对应 etcd 中的 /services/Hello.Greeter/v1
resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
	Target:   "Hello.Greeter/SayHello", // service/method
	Version:  "v1",                     // 目标版本
	Protocol: "GRPC",
	Payload:  helloPayload,
	Metadata: map[string]string{
		"authorization": "Bearer token123",
	},
})

// 调用 World 服务（HTTP 下游）
// 对应 etcd 中的 /services/Hello.World/v1
resp, err := client.CallUnary(ctx, &bridgev1.UnaryRequest{
	Target:   "Hello.World/Hello",       // service/method
	Version:  "v1",                     // 目标版本
	Protocol: "HTTP",
	Payload:  helloPayload,
	Metadata: map[string]string{
		"x-user-id": "user-456",
	},
})
```

### 9.4.5 etcd 数据结构说明

当前实现采用**每个实例独立 key**的注册模型，便于单个实例上下线时精确刷新缓存。

```
etcd 键值结构：

/services/                    # 前缀（Bridge 配置中的 etcd.prefix）
  ├── Hello.Greeter/          # 服务名（对应 Bridge target 的第一部分）
  │     └── v1/               # 版本（对应 Bridge target 的第三部分，省略时为 _default）
  │           ├── 127.0.0.1-50051   → {"address":"10.0.1.5:50051","weight":100,"protocol":"GRPC",...}
  │           └── 127.0.0.1-50052   → {"address":"10.0.1.6:50051","weight":100,"protocol":"GRPC",...}
  │
  ├── payment_service/
  │     └── v2/
  │           └── 10.0.1.6-50051    → {"address":"10.0.1.6:50051","weight":100,"protocol":"GRPC",...}
  │
  └── user_service/
        └── _default/
                  └── 10.0.1.7-8080 → {"address":"10.0.1.7:8080","weight":100,"protocol":"HTTP",...}
```

**target 解析规则**：
- `"order_service/CreateOrder"`       → service=`order_service`, version=`_default`, method=`CreateOrder`
- `"payment_service/Charge/v2"`       → service=`payment_service`, version=`v2`, method=`Charge`
- `"Hello.Greeter/SayHello/v1"`       → service=`Hello.Greeter`, version=`v1`, method=`SayHello`

**重要约定**：
- 注册端每个实例写入**单个 Endpoint 的 JSON**，不是数组。
- 发现端按 `{service}/{version}/` 前缀聚合所有实例，并过滤 `Healthy=true` 的节点。
- `version` 为空时，注册侧与发现侧都会规范化为 `_default`。

### 9.4.6 查看 etcd 数据

#### 使用 etcdctl 命令行工具

```bash
# 设置 etcd 版本（v3 API）
export ETCDCTL_API=3

# 查看所有服务注册信息（Bridge 使用的 /services/ 前缀）
etcdctl get /services/ --prefix --keys-only

# 查看具体服务的所有实例
etcdctl get /services/Hello.Greeter/v1/ --prefix

# 查看具体实例的注册信息
etcdctl get /services/Hello.Greeter/v1/127.0.0.1-50051

# 监听服务变更（watch）
etcdctl watch /services/ --prefix

# 查看租约列表（服务注册的租约）
etcdctl lease list

# 查看具体租约信息
etcdctl lease timetolive <lease-id>

# 删除具体实例注册（手动注销）
etcdctl del /services/Hello.Greeter/v1/127.0.0.1-50051

# 删除整个服务版本（谨慎操作）
etcdctl del /services/Hello.Greeter/v1/ --prefix
```

#### 使用 etcd 客户端代码查看

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/client/v3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"etcd-0:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. 查看所有服务 key
	resp, err := cli.Get(ctx, "/services/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== 已注册的服务 ===")
	for _, kv := range resp.Kvs {
		fmt.Printf("Key: %s
", kv.Key)
	}

	// 2. 查看具体服务的详细信息
	resp, err = cli.Get(ctx, "/services/Hello.Greeter/default")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("
=== Hello.Greeter 服务详情 ===")
	for _, kv := range resp.Kvs {
		fmt.Printf("Key: %s
", kv.Key)
		fmt.Printf("Value: %s
", string(kv.Value))

		// 解析 endpoint 信息
		var endpoints []map[string]interface{}
		if err := json.Unmarshal(kv.Value, &endpoints); err == nil {
			for _, ep := range endpoints {
				fmt.Printf("  Address:  %s
", ep["address"])
				fmt.Printf("  Weight:   %v
", ep["weight"])
				fmt.Printf("  Protocol: %s
", ep["protocol"])
				fmt.Printf("  Region:   %s
", ep["region"])
				fmt.Printf("  Healthy:  %v
", ep["healthy"])
			}
		}
	}
}
```

#### 使用 etcd 可视化工具（etcdkeeper）

```bash
# 1. 启动 etcdkeeper（etcd 可视化工具）
docker run -d --name etcdkeeper   -p 8080:8080   evildecay/etcdkeeper:latest

# 2. 浏览器访问 http://localhost:8080
# 3. 输入 etcd 地址（如 etcd-0:2379）连接
# 4. 在左侧树形结构中展开 /services/ 查看注册的服务
```

#### 使用 etcd 浏览器插件（VS Code）

安装 **etcd-explorer** 或 **etcd-workbench** 插件，配置 etcd 连接后直接浏览键值数据。

### 9.5 grpcurl 调试命令

```bash
# 查看 Bridge 服务列表
grpcurl -plaintext localhost:50052 list

# 调用健康检查
grpcurl -plaintext localhost:50052 bridge.v1.BridgeService/Health

# 调用一元方法（下游 gRPC 示例）
grpcurl -plaintext -d '{
  "target": "Hello.Greeter/SayHello/v1",
  "protocol": "GRPC",
  "timeout_ms": 3000,
  "metadata": {
    "authorization": "Bearer token123",
    "x-request-id": "req-456"
  }
}' localhost:50052 bridge.v1.BridgeService/CallUnary
```

**注意**：下游 gRPC 服务必须启用 `reflection.Register(gs)`，Bridge 才能通过 gRPC Reflection 获取输出类型并填充响应 `Any.TypeUrl`。如果下游未开启反射，客户端调用 `UnmarshalTo` 可能因类型校验失败而报错，此时可改用 `proto.Unmarshal(resp.Payload.Value, &res)` 绕过类型校验。

---

## 10. 性能优化清单

| 优化点 | 实现方式 | 预期收益 |
|--------|---------|---------|
| **零拷贝透传** | `rawBytesCodec` 直接透传 `[]byte`，Bridge 不反序列化业务消息 | 减少 CPU 和内存开销 |
| **反射类型缓存** | `methodTypeCache` 缓存下游方法输出类型 | 避免每次请求进行 gRPC Reflection |
| **对象池化** | `sync.Pool` 复用 `anypb.Any` 和 `[]byte` | 减少 GC 压力 30%+ |
| **零分配日志** | `zerolog` + `Event` 复用 | 日志路径零堆分配 |
| **连接复用** | `grpc.ClientConn` 全局缓存（Bridge 到下游） | 避免重复 TCP 握手 |
| **内存预分配** | `make([]byte, 0, estimatedSize)` | 减少切片扩容 |
| **无锁缓存** | `sync.Map` 存储路由表 | 避免 RWMutex 竞争 |
| **pprof 持续监控** | 暴露 `/debug/pprof` 端口 | 实时识别内存/CPU 热点 |
| **GODEBUG 调优** | `madvdontneed=0` | 优化 Linux 内存回收 |

---

## 11. 附录：关键设计决策

### A. 通信路径说明

```
业务方（gRPC Client）
    |
    | gRPC 协议（HTTP/2）
    v
Bridge Service（gRPC Server :50052）
    |
    | 根据 target 路由
    v
Protocol Handler（gRPC/HTTP）
    |
    | 对应协议
    v
下游微服务（gRPC/HTTP Server）
```

- **业务方 -> Bridge**：统一使用 **gRPC 协议**（HTTP/2）
- **Bridge -> 下游**：根据 `protocol` 字段选择对应协议（gRPC/HTTP）
- **协议透明**：业务方只需知道 Bridge 的 gRPC 接口，无需关心下游协议

### B. 错误处理策略

采用 **统一 `Status` 错误模型**：
- 所有错误（路由失败、熔断打开、下游超时）均通过响应体中的 `Status` 字段返回
- gRPC 层始终返回 `nil` error（HTTP/2 200）
- 业务方统一解析 `Status.Code` 判断业务/系统错误

**优势**：业务方无需处理多种错误格式，便于统一熔断和降级。

### C. 协议透传设计

使用 `google.protobuf.Any` + 自定义 `rawBytesCodec` + gRPC Reflection：
- Bridge 不感知业务消息的 Schema，不反序列化具体业务类型。
- gRPC 场景下，`rawBytesCodec` 直接透传 `Any.Value` 的原始字节到下游，避免 Bridge 对业务消息进行 protobuf 序列化/反序列化。
- 下游返回的原始字节被包装为 `anypb.Any` 后，Bridge 通过 gRPC Reflection 查询方法输出类型，填充 `TypeUrl`。
- 填充 `TypeUrl` 后，业务方客户端可以正常调用 `anypb.Any.UnmarshalTo(&resp)` 进行类型校验和解包。
- 如果下游未开启 gRPC Reflection，`UnmarshalTo` 可能因 `TypeUrl` 不匹配而失败，业务方可以改用 `proto.Unmarshal(resp.Payload.Value, &res)` 绕过类型校验。

### D. 熔断器粒度

按 **下游微服务 endpoint 地址** 维度管理熔断器：
- 单个下游节点故障不影响其他节点
- 内存中熔断器数量随 endpoint 线性增长（生产环境需监控）
- 支持半开状态自动探测恢复

### E. 路由缓存策略

**本地缓存 + etcd Watch 增量更新**：
- 查询路径 O(1)，无网络 I/O
- 最终一致性，启动时全量加载
- watch 断连时需有兜底重连机制

### F. 版本更新说明

| 包 | 当前版本 |
|---|---|
| `grpc-go` | **v1.81.1** |
| `protobuf` | **v1.36.11** |
| `etcd/client/v3` | **v3.6.12** |
| `otel` | **v1.44.0** |
| `resty/v2` | **v2.17.2** |
| `gobreaker` | **v2.4.0** |
| `prometheus` | **v1.20.5** |
| `zerolog` | **v1.35.1** |
| `viper` | **v1.21.0** |
| `Go` | **1.25.8** |

---

*文档版本: v3.1 | 最后更新: 2026-06-14*  
*Go版本: 1.25.8+ | 依赖版本: 与 go.mod 保持一致*
