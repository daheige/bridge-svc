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
	ServiceName          string        `mapstructure:"service_name"`           // 服务名
	ServiceVersion       string        `mapstructure:"service_version"`        // 服务版本
}

// EtcdConfig etcd 服务发现配置
type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`    // etcd 集群地址
	DialTimeout time.Duration `mapstructure:"dial_timeout"` // 连接超时
	Prefix      string        `mapstructure:"prefix"`       // 服务注册前缀
}

// ProtocolConfig 协议层配置（Bridge 到下游微服务的协议配置）
type ProtocolConfig struct {
	DefaultTimeout time.Duration `mapstructure:"default_timeout"` // 默认超时
	Grpc           GrpcConfig    `mapstructure:"grpc"`            // gRPC 下游配置
	HTTP           HTTPConfig    `mapstructure:"http"`            // HTTP 下游配置
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
	LogLevel       string `mapstructure:"log_level"`      // 日志级别
	TraceEndpoint  string `mapstructure:"trace_endpoint"` // OTLP Trace 接收端
	MetricsPort    int    `mapstructure:"metrics_port"`   // Prometheus 指标端口
	ServiceName    string `mapstructure:"-"`              // 服务名
	ServiceVersion string `mapstructure:"-"`              // 服务版本
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
