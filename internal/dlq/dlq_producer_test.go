package dlq_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/dlq"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
)

func TestProducer_Send(t *testing.T) {
	var kafkaProducer *KafkaProducerMock
	var suite dlq.Producer

	setup := func() {
		kafkaProducer = new(KafkaProducerMock)
		suite = dlq.NewProducer(kafkaProducer, "events.dlq")
	}

	t.Run("should produce rejected event envelope", func(t *testing.T) {
		setup()
		evt := event.Event{
			EventID:   "evt-1",
			EventType: "monitoring.alert",
			TenantID:  "tenant-a",
			Timestamp: "2026-05-01T00:00:00Z",
			Payload:   map[string]any{"severity": "high"},
		}
		errors := []string{"event_id is required"}

		kafkaProducer.On("Produce", mock.Anything,
			mock.MatchedBy(func(msg kafka.ProducerMessage[event.RejectedEvent]) bool {
				return msg.Topic == "events.dlq" &&
					msg.Value.Event.EventID == "evt-1" &&
					len(msg.Value.Errors) == 1 &&
					msg.Value.Errors[0] == "event_id is required"
			}),
		).Return(nil)

		err := suite.Send(context.Background(), evt, errors)

		require.NoError(t, err)
		kafkaProducer.AssertExpectations(t)
	})

	t.Run("should return error when kafka produce fails", func(t *testing.T) {
		setup()
		kafkaProducer.On("Produce", mock.Anything, mock.Anything).Return(assert.AnError)

		err := suite.Send(context.Background(), event.Event{}, []string{"error"})

		require.Error(t, err)
		kafkaProducer.AssertExpectations(t)
	})
}

func TestProducer_Close(t *testing.T) {
	kafkaProducer := new(KafkaProducerMock)
	suite := dlq.NewProducer(kafkaProducer, "events.dlq")

	kafkaProducer.On("Flush").Return()
	kafkaProducer.On("Close").Return()

	suite.Close()

	kafkaProducer.AssertExpectations(t)
	kafkaProducer.AssertNumberOfCalls(t, "Flush", 1)
	kafkaProducer.AssertNumberOfCalls(t, "Close", 1)
}
