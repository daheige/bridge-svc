package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/daheige/registry"
	"github.com/daheige/registry/etcd"

	"github.com/daheige/bridge-svc/internal/config"
	"github.com/daheige/bridge-svc/internal/server"
)

func main() {
	cfg, err := config.Load("config/bridge.yaml")
	if err != nil {
		log.Fatal().Err(err).Msg("load config failed")
	}

	// 创建 gRPC 服务并获取实际监听地址，用于 etcd 注册。
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("create server failed")
	}

	// 将 0.0.0.0 / [::] 等通配监听地址转换为可访问地址（本地测试使用 127.0.0.1）。
	// 生产环境建议配置独立 advertise_addr。
	advertiseAddr := toAdvertiseAddr(srv.Addr())

	serviceName := cfg.Server.ServiceName
	if serviceName == "" {
		serviceName = "bridge-svc"
	}

	reg, err := etcd.NewServiceRegistry(
		cfg.Etcd.Endpoints,
		cfg.Etcd.Prefix,
		serviceName,
		registry.Endpoint{
			Address:  advertiseAddr,
			Protocol: registry.ProtocolGRPC,
			Version:  cfg.Server.ServiceVersion,
			Healthy:  true,
			Weight:   100,
			Tags: map[string]string{
				"service": serviceName,
				"version": cfg.Observability.ServiceVersion,
			},
		},
		etcd.WithTTL(10),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("create etcd registry failed")
	}

	if err := reg.Register(); err != nil {
		log.Fatal().Err(err).Msg("register bridge-svc to etcd failed")
	}
	log.Info().Str("service", serviceName).Str("addr", advertiseAddr).Msg("registered bridge-svc to etcd")

	// 优雅关闭：监听系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// 平滑退出
	defer func() {
		srv.Stop()
		log.Info().Msg("bridge-svc stopped")
	}()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatal().Err(err).Msg("server error")
		}
	case sig := <-sigCh:
		log.Info().Str("signal", sig.String()).Msg("shutdown signal received, graceful shutdown")
	}

	// 注销服务并停止 gRPC 服务。
	if err := reg.Deregister(); err != nil {
		log.Error().Err(err).Msg("deregister bridge-svc from etcd failed")
	}

}

// toAdvertiseAddr 把监听地址中的通配地址替换为本地回环地址，
// 便于同机客户端通过 etcd 发现后连接。生产环境应配置真实可访问地址。
func toAdvertiseAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
}
