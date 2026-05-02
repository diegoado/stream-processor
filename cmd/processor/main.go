package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/diegoado/stream-processor/internal/processor"
	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/logger"
)

func main() {
	log := logger.Get("main")

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	p := processor.NewProcessor(cfg)
	if err = p.Start(ctx); err != nil {
		log.Error("processor error", slog.Any("error", err))
		panic(err)
	}
}
