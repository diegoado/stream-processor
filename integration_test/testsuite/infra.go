//go:build integration

package testsuite

import (
	"context"
	"fmt"
	"os"

	"github.com/diegoado/stream-processor/integration_test/testsuite/containers"
)

// Init sets environment variables from container endpoints for the test suite.
func Init(infra *containers.Infrastructure) {
	ctx := context.Background()

	broker, err := infra.KafkaBroker(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to get kafka broker: %w", err))
	}

	lsEndpoint, err := infra.LocalStackEndpoint(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to get localstack endpoint: %w", err))
	}

	_ = os.Setenv("KAFKA_BOOTSTRAP_SERVERS", broker)
	_ = os.Setenv("AWS_ENDPOINT", lsEndpoint)
	_ = os.Setenv("AWS_REGION", "us-east-1")
	_ = os.Setenv("AWS_ACCESS_KEY_ID", "test")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	_ = os.Setenv("S3_SCHEMA_BUCKET", "stream-processor")
	_ = os.Setenv("S3_SCHEMA_KEY", "schemas/event_schema.json")

	_ = os.Setenv("PROCESSOR_EVENTS_TOPIC", "events")
	_ = os.Setenv("PROCESSOR_ACCEPTED_EVENTS_TOPIC", "arn:aws:sns:us-east-1:000000000000:events-topic")
	_ = os.Setenv("PROCESSOR_REJECTED_EVENTS_TOPIC", "events.dlq")
	_ = os.Setenv("PROCESSOR_POLL_TIMEOUT", "100ms")
}
