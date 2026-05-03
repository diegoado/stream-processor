//go:build integration

package steps

import (
	"fmt"
	"time"

	"github.com/cucumber/godog"

	"github.com/diegoado/stream-processor/integration_test/testsuite"
)

// ProcessingScenarioCtx holds state for event processing scenarios.
type ProcessingScenarioCtx struct {
	ScenarioCtx
}

// InitializeProcessingScenario registers step definitions for event processing.
func InitializeProcessingScenario(ctx *godog.ScenarioContext) {
	sc := &ProcessingScenarioCtx{ScenarioCtx{client: testsuite.NewSuiteClient()}}
	sc.InitializeCommon(ctx)

	ctx.Then(`^the event should be received in the SNS topic$`, sc.theEventShouldBeReceivedInTheSNSTopic)
}

func (sc *ProcessingScenarioCtx) theEventShouldBeReceivedInTheSNSTopic() error {
	received, err := sc.client.ReceiveSNSMessage(sc.event.EventID, 10*time.Second)
	if err != nil {
		return fmt.Errorf("expected event in SNS topic: %w", err)
	}

	if received.EventID != sc.event.EventID {
		return fmt.Errorf("event_id mismatch: expected %q, got %q", sc.event.EventID, received.EventID)
	}
	return nil
}
