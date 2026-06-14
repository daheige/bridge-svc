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
