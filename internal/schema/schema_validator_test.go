package schema_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/schema"
	"github.com/diegoado/stream-processor/pkg/event"
)

func TestValidator_ValidateAndSanitize(t *testing.T) {
	var suite schema.Validator

	setup := func() {
		t.Helper()
		data, err := os.ReadFile("../../schemas/event_schema.json")
		require.NoError(t, err)

		suite, err = schema.NewValidator(data, "etag-1")
		require.NoError(t, err)
	}

	validTestCases := []struct {
		name            string
		evt             event.Event
		expectedKeys    []string
		notExpectedKeys []string
	}{
		{
			name: "should accept and sanitize valid monitoring.alert for tenant-a",
			evt: event.Event{
				EventID: "550e8400-e29b-41d4-a716-446655440000", EventType: "monitoring.alert",
				TenantID: "tenant-a", Timestamp: "2026-05-01T00:00:00Z",
				Payload: map[string]any{"severity": "high", "message": "alert", "source": "cpu", "extra": "stripped"},
			},
			expectedKeys:    []string{"severity", "message", "source"},
			notExpectedKeys: []string{"extra"},
		},
		{
			name: "should accept valid user.action for unknown tenant with default fields",
			evt: event.Event{
				EventID: "550e8400-e29b-41d4-a716-446655440000", EventType: "user.action",
				TenantID: "tenant-new", Timestamp: "2026-05-01T00:00:00Z",
				Payload: map[string]any{"action": "click", "user_id": "u1", "extra": "stripped"},
			},
			expectedKeys:    []string{"action", "user_id"},
			notExpectedKeys: []string{"extra"},
		},
	}

	invalidTestCases := []struct {
		name string
		evt  event.Event
	}{
		{
			name: "should reject missing event_id",
			evt: event.Event{
				EventType: "monitoring.alert", TenantID: "tenant-a",
				Timestamp: "2026-05-01T00:00:00Z",
				Payload:   map[string]any{"severity": "high", "message": "alert"},
			},
		},
		{
			name: "should reject bad timestamp format",
			evt: event.Event{
				EventID: "550e8400-e29b-41d4-a716-446655440000", EventType: "monitoring.alert",
				TenantID: "tenant-a", Timestamp: "not-a-date",
				Payload: map[string]any{"severity": "high", "message": "alert"},
			},
		},
		{
			name: "should reject empty payload missing required fields",
			evt: event.Event{
				EventID: "550e8400-e29b-41d4-a716-446655440000", EventType: "monitoring.alert",
				TenantID: "tenant-a", Timestamp: "2026-05-01T00:00:00Z",
				Payload: map[string]any{},
			},
		},
	}

	for _, tc := range validTestCases {
		t.Run(tc.name, func(t *testing.T) {
			setup()

			sanitized, errors, err := suite.ValidateAndSanitize(tc.evt)

			require.NoError(t, err)
			assert.Empty(t, errors)
			require.NotNil(t, sanitized)
			for _, key := range tc.expectedKeys {
				assert.Contains(t, sanitized.Payload, key)
			}
			for _, key := range tc.notExpectedKeys {
				assert.NotContains(t, sanitized.Payload, key)
			}
		})
	}

	for _, tc := range invalidTestCases {
		t.Run(tc.name, func(t *testing.T) {
			setup()

			sanitized, errors, err := suite.ValidateAndSanitize(tc.evt)

			require.NoError(t, err)
			require.NotEmpty(t, errors)
			assert.Nil(t, sanitized)
		})
	}
}

func TestValidator_Update(t *testing.T) {
	data, err := os.ReadFile("../../schemas/event_schema.json")
	require.NoError(t, err)

	suite, err := schema.NewValidator(data, "etag-1")
	require.NoError(t, err)

	t.Run("should update schema and etag", func(t *testing.T) {
		updateErr := suite.Update(data, "etag-2")

		require.NoError(t, updateErr)
		assert.Equal(t, "etag-2", suite.ETag())
	})

	t.Run("should return error for invalid schema", func(t *testing.T) {
		updateErr := suite.Update([]byte("invalid"), "etag-3")

		require.Error(t, updateErr)
		assert.Equal(t, "etag-2", suite.ETag())
	})
}
