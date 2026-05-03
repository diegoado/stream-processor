package schema_test

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/event"
)

type S3ClientMock struct {
	mock.Mock
}

func (m *S3ClientMock) HeadObject(ctx context.Context, bucket, key string) (*aws.HeadObjectOutput, error) {
	args := m.Called(ctx, bucket, key)
	out, _ := args.Get(0).(*aws.HeadObjectOutput)
	return out, args.Error(1)
}

func (m *S3ClientMock) GetObject(ctx context.Context, bucket, key string) (*aws.GetObjectOutput, error) {
	args := m.Called(ctx, bucket, key)
	out, _ := args.Get(0).(*aws.GetObjectOutput)
	return out, args.Error(1)
}

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
