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
