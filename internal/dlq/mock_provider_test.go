package dlq_test

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
)

type KafkaProducerMock struct {
	mock.Mock
}

func (m *KafkaProducerMock) Produce(ctx context.Context, msg kafka.ProducerMessage[event.RejectedEvent]) error {
	return m.Called(ctx, msg).Error(0)
}

func (m *KafkaProducerMock) Flush() {
	m.Called()
}

func (m *KafkaProducerMock) Close() {
	m.Called()
}
