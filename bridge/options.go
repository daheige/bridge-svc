package bridge

import (
	"time"

	"google.golang.org/grpc"
)

// ClientOptions 创建 Client 时的选项。
type ClientOptions struct {
	DialOptions    []grpc.DialOption
	DefaultTimeout time.Duration
}

// ClientOption 用于配置 Client。
type ClientOption func(*ClientOptions)

// WithDialOptions 追加 gRPC 拨号选项。
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *ClientOptions) {
		o.DialOptions = append(o.DialOptions, opts...)
	}
}

// WithDefaultTimeout 设置默认调用超时。
func WithDefaultTimeout(d time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.DefaultTimeout = d
	}
}
