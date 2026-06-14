package observability

import (
	"fmt"

	"github.com/daheige/bridge-svc/internal/config"
)

// Init 初始化所有可观测性组件
func Init(cfg config.ObservabilityConfig) error {
	InitLogger(cfg.LogLevel)

	if err := InitTracer(cfg); err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}

	return nil
}
