package handler_test

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/diegoado/stream-processor/pkg/event"
)

type ValidatorMock struct {
	mock.Mock
}

func (m *ValidatorMock) ValidateAndSanitize(evt event.Event) (*event.Event, []string, error) {
	args := m.Called(evt)
	sanitized, _ := args.Get(0).(*event.Event)
	errors, _ := args.Get(1).([]string)
	return sanitized, errors, args.Error(2)
}

func (m *ValidatorMock) Update(data []byte, etag string) error {
	return m.Called(data, etag).Error(0)
}

func (m *ValidatorMock) ETag() string {
	return m.Called().String(0)
}

type PublisherMock struct {
	mock.Mock
}

func (m *PublisherMock) Publish(ctx context.Context, evt *event.Event) error {
	return m.Called(ctx, evt).Error(0)
}

type RejecterMock struct {
	mock.Mock
}

func (m *RejecterMock) Send(ctx context.Context, original event.Event, errors []string) error {
	return m.Called(ctx, original, errors).Error(0)
}

func (m *RejecterMock) Close() {
	m.Called()
}
