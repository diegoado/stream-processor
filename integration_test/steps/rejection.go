//go:build integration

package steps

import (
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/diegoado/stream-processor/integration_test/testsuite"
	"github.com/diegoado/stream-processor/pkg/event"
)

// RejectionScenarioCtx holds state for event rejection scenarios.
type RejectionScenarioCtx struct {
	ScenarioCtx
	rejected *event.RejectedEvent
}

// InitializeRejectionScenario registers step definitions for event rejection.
func InitializeRejectionScenario(ctx *godog.ScenarioContext) {
	sc := &RejectionScenarioCtx{ScenarioCtx: ScenarioCtx{client: testsuite.NewSuiteClient()}}
	sc.InitializeCommon(ctx)

	ctx.Then(`^the event should appear in the "([^"]*)" topic$`, sc.theEventShouldAppearInTopic)
	ctx.Then(`^the DLQ message should contain error "([^"]*)"$`, sc.theDlqMessageShouldContainError)
}

func (sc *RejectionScenarioCtx) theEventShouldAppearInTopic(topic string) error {
	rejected, err := sc.client.ConsumeDlqMessage(topic, sc.event.EventID, 10*time.Second)
	if err != nil {
		return fmt.Errorf("expected message in %q but got none: %w", topic, err)
	}
	sc.rejected = rejected
	return nil
}

func (sc *RejectionScenarioCtx) theDlqMessageShouldContainError(expected string) error {
	if sc.rejected == nil {
		return fmt.Errorf("no DLQ message captured")
	}
	for _, e := range sc.rejected.Errors {
		if strings.Contains(e, expected) {
			return nil
		}
	}
	return fmt.Errorf("DLQ errors %v do not contain %q", sc.rejected.Errors, expected)
}
