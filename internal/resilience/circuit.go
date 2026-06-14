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
