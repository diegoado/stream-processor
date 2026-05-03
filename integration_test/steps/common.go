//go:build integration

package steps

import (
	"encoding/json"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/diegoado/stream-processor/integration_test/testsuite"
	"github.com/diegoado/stream-processor/pkg/event"
)

// ScenarioCtx holds shared state across steps within a scenario.
type ScenarioCtx struct {
	client *testsuite.SuiteClient
	event  event.Event
}

func (sc *ScenarioCtx) anEventWithPayload(payload *godog.DocString) error {
	return json.Unmarshal([]byte(payload.Content), &sc.event)
}

func (sc *ScenarioCtx) theEventIsProducedToTopic(topic string) error {
	if err := sc.client.ProduceEvent(topic, sc.event); err != nil {
		return fmt.Errorf("failed to produce event: %w", err)
	}
	sc.client.KafkaProducer.Flush(1000)
	return nil
}

// InitializeCommon registers shared step definitions.
func (sc *ScenarioCtx) InitializeCommon(ctx *godog.ScenarioContext) {
	ctx.Given(`^an event with payload:$`, sc.anEventWithPayload)
	ctx.When(`^the event is produced to the "([^"]*)" topic$`, sc.theEventIsProducedToTopic)
}
