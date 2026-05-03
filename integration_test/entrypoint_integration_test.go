//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/diegoado/stream-processor/integration_test/testsuite"
	"github.com/diegoado/stream-processor/integration_test/testsuite/containers"
	"github.com/diegoado/stream-processor/internal/processor"
	"github.com/diegoado/stream-processor/pkg/config"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())

	infra, err := containers.Setup(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to setup infrastructure: %w", err))
	}

	testsuite.Init(infra)

	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load config: %w", err))
	}

	go func() {
		_ = processor.NewProcessor(cfg).Start(ctx)
	}()

	// Subscribe returns immediately
	// but the consumer only starts receiving messages
	// after the broker assigns partitions, which requires a full rebalance cycle.
	time.Sleep(5 * time.Second)
	exitCode := m.Run()

	cancel()
	infra.Teardown(context.Background())
	os.Exit(exitCode)
}
