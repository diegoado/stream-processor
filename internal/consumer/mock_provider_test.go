package consumer_test

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
)

type KafkaConsumerMock struct {
	mock.Mock
}

func (m *KafkaConsumerMock) Subscribe(topic string) error {
	return m.Called(topic).Error(0)
}

func (m *KafkaConsumerMock) Poll(timeout time.Duration) (*kafka.Message[event.Event], error) {
	args := m.Called(timeout)
	msg, _ := args.Get(0).(*kafka.Message[event.Event])
	return msg, args.Error(1)
}

func (m *KafkaConsumerMock) Commit() error {
	return m.Called().Error(0)
}

func (m *KafkaConsumerMock) Close() error {
	return m.Called().Error(0)
}

type HandlerMock struct {
	mock.Mock
}

func (m *HandlerMock) Handle(ctx context.Context, evt event.Event) error {
	return m.Called(ctx, evt).Error(0)
}
