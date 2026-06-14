package observability

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// InitLogger 初始化 zerolog
// 零分配 JSON 日志，便于采集到 OpenObserve
func InitLogger(level string) {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	l, err := zerolog.ParseLevel(level)
	if err != nil {
		l = zerolog.InfoLevel
	}

	log.Logger = zerolog.New(os.Stdout).
		Level(l).
		With().
		Timestamp().
		Caller().
		Logger()
}
