package bridge

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Service 封装单个下游 gRPC 服务的连接与调用。
type Service struct {
	cfg  *ServiceConfig
	conn *grpc.ClientConn
	opts ClientOptions
}

func newService(cfg *ServiceConfig, opts ClientOptions) (*Service, error) {
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts.DialOptions...)

	conn, err := grpc.NewClient(cfg.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Target, err)
	}

	return &Service{
		cfg:  cfg,
		conn: conn,
		opts: opts,
	}, nil
}

// Invoke 调用该服务下的指定方法。
// method 为方法名，如 SayHello。
func (s *Service) Invoke(ctx context.Context, method string, req, resp proto.Message, opts ...grpc.CallOption) error {
	fullMethod := method
	if !strings.HasPrefix(method, "/") {
		fullMethod = fmt.Sprintf("/%s/%s", s.cfg.fullServiceName(), method)
	}

	return s.InvokeFull(ctx, fullMethod, req, resp, opts...)
}

// InvokeFull 通过完整 gRPC 方法路径调用，如 "/Hello.Greeter/SayHello"。
func (s *Service) InvokeFull(ctx context.Context, fullMethod string, req, resp proto.Message, opts ...grpc.CallOption) error {
	ctx = mergeMetadata(ctx, s.cfg.Metadata)
	ctx, cancel := withTimeout(ctx, s.cfg.Timeout, s.opts.DefaultTimeout)
	defer cancel()

	return s.conn.Invoke(ctx, fullMethod, req, resp, opts...)
}

// Conn 返回该服务底层的 gRPC 连接。
func (s *Service) Conn() *grpc.ClientConn {
	return s.conn
}

// Config 返回该服务的配置。
func (s *Service) Config() ServiceConfig {
	if s.cfg == nil {
		return ServiceConfig{}
	}

	return *s.cfg
}

// Close 关闭该服务的连接。
func (s *Service) Close() error {
	if s.conn == nil {
		return nil
	}

	return s.conn.Close()
}

// Target 返回下游目标地址。
func (s *Service) Target() string {
	if s.cfg == nil {
		return ""
	}

	return s.cfg.Target
}

// Name 返回服务逻辑名。
func (s *Service) Name() string {
	if s.cfg == nil {
		return ""
	}

	return s.cfg.Name
}
