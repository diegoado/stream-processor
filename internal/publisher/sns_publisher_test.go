package publisher_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/publisher"
	"github.com/diegoado/stream-processor/pkg/event"
)

func TestPublisher_Publish(t *testing.T) {
	var snsClient *SNSClientMock
	var suite publisher.Publisher

	setup := func() {
		snsClient = new(SNSClientMock)
		suite = publisher.NewPublisher(snsClient, "arn:aws:sns:us-east-1:000000000000:events-topic")
	}

	t.Run("should publish event with tenant_id attribute", func(t *testing.T) {
		setup()
		evt := &event.Event{
			EventID:   "evt-1",
			EventType: "monitoring.alert",
			TenantID:  "tenant-a",
			Timestamp: "2026-05-01T00:00:00Z",
			Payload:   map[string]any{"severity": "high", "message": "alert"},
		}
		snsClient.On("Publish", mock.Anything, "arn:aws:sns:us-east-1:000000000000:events-topic",
			evt, map[string]string{"tenant_id": "tenant-a"}).Return(nil)

		err := suite.Publish(context.Background(), evt)

		require.NoError(t, err)
		snsClient.AssertExpectations(t)
	})

	t.Run("should return error when SNS fails", func(t *testing.T) {
		setup()
		evt := &event.Event{EventID: "evt-1", TenantID: "tenant-a"}
		snsClient.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(assert.AnError)

		err := suite.Publish(context.Background(), evt)

		require.Error(t, err)
		snsClient.AssertExpectations(t)
	})
}
