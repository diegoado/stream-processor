package publisher_test

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type SNSClientMock struct {
	mock.Mock
}

func (m *SNSClientMock) Publish(
	ctx context.Context,
	topicARN string,
	message any,
	attributes map[string]string,
) error {
	return m.Called(ctx, topicARN, message, attributes).Error(0)
}
