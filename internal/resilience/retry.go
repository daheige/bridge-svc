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
