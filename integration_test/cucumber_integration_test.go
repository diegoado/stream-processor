//go:build integration

package integration_test

import (
	"testing"

	"github.com/cucumber/godog"

	"github.com/diegoado/stream-processor/integration_test/steps"
)

func TestEventProcessing(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: steps.InitializeProcessingScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features/event_processing.feature"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("failed to run event processing feature tests")
	}
}

func TestEventRejection(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: steps.InitializeRejectionScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features/event_rejection.feature"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("failed to run event rejection feature tests")
	}
}
