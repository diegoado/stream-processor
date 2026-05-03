package utilities_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/diegoado/stream-processor/pkg/event"
	suite "github.com/diegoado/stream-processor/pkg/utilities"
)

func TestDoSanitizeEventPayload(t *testing.T) {
	baseEvent := &event.Event{
		EventID:   "evt-1",
		EventType: "monitoring.alert",
		TenantID:  "tenant-a",
		Timestamp: "2026-05-01T00:00:00Z",
	}

	testCases := []struct {
		name            string
		payload         map[string]any
		allowed         map[string]struct{}
		expectedPayload map[string]any
	}{
		{
			name:            "should keep all allowed fields",
			payload:         map[string]any{"severity": "high", "message": "alert"},
			allowed:         map[string]struct{}{"severity": {}, "message": {}},
			expectedPayload: map[string]any{"severity": "high", "message": "alert"},
		},
		{
			name:            "should strip extra fields",
			payload:         map[string]any{"severity": "high", "extra": "stripped"},
			allowed:         map[string]struct{}{"severity": {}},
			expectedPayload: map[string]any{"severity": "high"},
		},
		{
			name:            "should return empty payload when no fields allowed",
			payload:         map[string]any{"severity": "high"},
			allowed:         map[string]struct{}{},
			expectedPayload: map[string]any{},
		},
		{
			name:            "should return empty payload when source is empty",
			payload:         map[string]any{},
			allowed:         map[string]struct{}{"severity": {}},
			expectedPayload: map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evt := *baseEvent
			evt.Payload = tc.payload

			result := suite.DoSanitizeEventPayload(&evt, tc.allowed)

			assert.Equal(t, baseEvent.EventID, result.EventID)
			assert.Equal(t, baseEvent.EventType, result.EventType)
			assert.Equal(t, baseEvent.TenantID, result.TenantID)
			assert.Equal(t, baseEvent.Timestamp, result.Timestamp)
			assert.Equal(t, tc.expectedPayload, result.Payload)
		})
	}

	t.Run("should not mutate original event", func(t *testing.T) {
		evt := &event.Event{
			EventID: "evt-1",
			Payload: map[string]any{"severity": "high", "extra": "field"},
		}

		_ = suite.DoSanitizeEventPayload(evt, map[string]struct{}{"severity": {}})

		assert.Len(t, evt.Payload, 2)
		assert.Contains(t, evt.Payload, "extra")
	})
}
