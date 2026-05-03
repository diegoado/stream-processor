package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/consumer"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
)

func TestConsumer_Start(t *testing.T) {
	var kafkaConsumer *KafkaConsumerMock
	var handler *HandlerMock
	var suite consumer.Consumer

	setup := func() {
		kafkaConsumer = new(KafkaConsumerMock)
		handler = new(HandlerMock)
		suite = consumer.NewConsumer(kafkaConsumer, handler, "events", 100*time.Millisecond)
	}

	t.Run("should return error when subscribe fails", func(t *testing.T) {
		setup()
		kafkaConsumer.On("Subscribe", "events").Return(assert.AnError)

		err := suite.Start(context.Background())

		require.Error(t, err)
		kafkaConsumer.AssertExpectations(t)
	})

	t.Run("should poll and handle event then stop on context cancel", func(t *testing.T) {
		setup()
		ctx, cancel := context.WithCancel(context.Background())

		evt := event.Event{
			EventID: "evt-1", EventType: "monitoring.alert",
			TenantID: "tenant-a", Timestamp: "2026-05-01T00:00:00Z",
			Payload: map[string]any{"severity": "high"},
		}

		kafkaConsumer.On("Subscribe", "events").Return(nil)
		kafkaConsumer.On("Poll", mock.Anything).Return(
			&kafka.Message[event.Event]{Value: evt}, nil,
		).Once()
		kafkaConsumer.On("Poll", mock.Anything).Return(
			(*kafka.Message[event.Event])(nil), nil,
		).Run(func(_ mock.Arguments) { cancel() })
		kafkaConsumer.On("Commit").Return(nil)
		kafkaConsumer.On("Close").Return(nil)

		handler.On("Handle", mock.Anything, evt).Return(nil)

		err := suite.Start(ctx)

		require.NoError(t, err)
		kafkaConsumer.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
}
