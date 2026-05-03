package schema_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/schema"
)

func TestDataSchema_Build(t *testing.T) {
	data, err := os.ReadFile("../../schemas/event_schema.json")
	require.NoError(t, err)

	suite, err := schema.BuildDataSchema(data)
	require.NoError(t, err)

	t.Run("should return default fields for known event type", func(t *testing.T) {
		fields := suite.AllowedFields("unknown-tenant", "monitoring.alert")

		assert.Contains(t, fields, "severity")
		assert.Contains(t, fields, "source")
		assert.Contains(t, fields, "message")
	})

	t.Run("should return nil for unknown event type", func(t *testing.T) {
		fields := suite.AllowedFields("tenant-a", "unknown.type")

		assert.Nil(t, fields)
	})

	overrideTestCases := []struct {
		name      string
		tenantID  string
		eventType string
		expected  []string
	}{
		{
			name:      "should return tenant-a monitoring.alert override with alert_url",
			tenantID:  "tenant-a",
			eventType: "monitoring.alert",
			expected:  []string{"severity", "source", "message", "alert_url"},
		},
		{
			name:      "should return tenant-a transaction.auth override with risk_score",
			tenantID:  "tenant-a",
			eventType: "transaction.auth",
			expected:  []string{"transaction_id", "amount", "currency", "status", "risk_score"},
		},
		{
			name:      "should return tenant-b user.action override with session_id",
			tenantID:  "tenant-b",
			eventType: "user.action",
			expected:  []string{"action", "user_id", "resource", "session_id"},
		},
		{
			name:      "should return tenant-c webhook.dispatched override with callback_id",
			tenantID:  "tenant-c",
			eventType: "webhook.dispatched",
			expected:  []string{"webhook_id", "endpoint", "http_status", "callback_id"},
		},
	}

	for _, tc := range overrideTestCases {
		t.Run(tc.name, func(t *testing.T) {
			fields := suite.AllowedFields(tc.tenantID, tc.eventType)

			require.NotNil(t, fields)
			assert.Len(t, fields, len(tc.expected))

			for _, key := range tc.expected {
				assert.Contains(t, fields, key)
			}
		})
	}

	t.Run("should fallback to default when tenant has no override for event type", func(t *testing.T) {
		fields := suite.AllowedFields("tenant-a", "user.action")

		assert.Contains(t, fields, "action")
		assert.Contains(t, fields, "user_id")
		assert.Contains(t, fields, "resource")
		assert.NotContains(t, fields, "session_id")
	})
}

func TestDataSchema_BuildWithInvalidJson(t *testing.T) {
	_, err := schema.BuildDataSchema([]byte("invalid"))

	assert.Error(t, err)
}
