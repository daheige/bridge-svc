package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daheige/hephfx/hestia"
	"github.com/rs/zerolog/log"

	"github.com/daheige/hephfx/hestia/etcd"

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
	// advertiseAddr := toAdvertiseAddr(srv.Addr())
	advertiseAddr, err := hestia.Resolve(srv.Addr())
	if err != nil {
		log.Fatal().Err(err).Msg("resolve addr failed")
	}

	serviceName := cfg.Server.ServiceName
	if serviceName == "" {
		serviceName = "bridge-svc"
	}
	regEntry, err := etcd.NewRegistry([]string{
		"127.0.0.1:12379",
	},
		etcd.WithDialTimeout(10*time.Second),
		etcd.WithPrefix("services"),
	)
	if err != nil {
		log.Fatal().Msgf("failed to new service registry", err)
	}
	regService := &hestia.Service{
		Network:  "tcp",
		Name:     serviceName,
		Address:  advertiseAddr,
		Version:  "v1",
		Created:  time.Now().Format("2006-01-02 15:04:05"),
		Protocol: "GRPC",
		Healthy:  true,
	}
	err = regEntry.Register(context.Background(), regService)
	if err != nil {
		log.Fatal().Msgf("failed to register service", err)
	}

	// 注销服务并停止 gRPC 服务。
	defer regEntry.Deregister(context.Background(), regService)

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
