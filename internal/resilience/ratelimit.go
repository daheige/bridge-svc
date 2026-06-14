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
