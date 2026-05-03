package schema_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/schema"
	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/config"
)

func TestLoader_Load(t *testing.T) {
	var s3Client *S3ClientMock
	var suite schema.Loader

	cfg := config.SchemaConfig{
		Bucket:          "test-bucket",
		Key:             "schemas/event_schema.json",
		RefreshInterval: time.Minute,
	}

	setup := func() {
		s3Client = new(S3ClientMock)
		suite = schema.NewLoader(s3Client, cfg)
	}

	t.Run("should load schema from S3", func(t *testing.T) {
		setup()
		etag := "etag-1"
		s3Client.On("GetObject", mock.Anything, "test-bucket", "schemas/event_schema.json").
			Return(&aws.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(`{"type":"object"}`)),
				ETag: &etag,
			}, nil)

		data, returnedEtag, err := suite.Load(context.Background())

		require.NoError(t, err)
		assert.JSONEq(t, `{"type":"object"}`, string(data))
		assert.Equal(t, "etag-1", returnedEtag)
		s3Client.AssertExpectations(t)
	})

	t.Run("should return error when S3 GetObject fails", func(t *testing.T) {
		setup()
		s3Client.On("GetObject", mock.Anything, mock.Anything, mock.Anything).
			Return((*aws.GetObjectOutput)(nil), assert.AnError)

		_, _, err := suite.Load(context.Background())

		require.Error(t, err)
		s3Client.AssertExpectations(t)
	})
}

func TestLoader_StartAutoRefresh(t *testing.T) {
	var s3Client *S3ClientMock
	var validator *ValidatorMock
	var suite schema.Loader

	cfg := config.SchemaConfig{
		Bucket:          "test-bucket",
		Key:             "schemas/event_schema.json",
		RefreshInterval: 50 * time.Millisecond,
	}

	setup := func() {
		s3Client = new(S3ClientMock)
		validator = new(ValidatorMock)
		suite = schema.NewLoader(s3Client, cfg)
	}

	t.Run("should refresh when ETag changes", func(t *testing.T) {
		setup()

		newETag := "etag-2"
		s3Client.On("HeadObject", mock.Anything, "test-bucket", "schemas/event_schema.json").
			Return(&aws.HeadObjectOutput{ETag: &newETag}, nil)
		s3Client.On("GetObject", mock.Anything, "test-bucket", "schemas/event_schema.json").
			Return(&aws.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(`{"type":"object"}`)),
				ETag: &newETag,
			}, nil).Once()

		validator.On("ETag").Return("etag-1").Once()
		validator.On("ETag").Return("etag-2")
		validator.On("Update", mock.Anything, "etag-2").Return(nil).Once()

		ctx, cancel := context.WithCancel(context.Background())
		suite.StartAutoRefresh(ctx, validator)

		time.Sleep(150 * time.Millisecond)
		cancel()

		validator.AssertNumberOfCalls(t, "Update", 1)
	})

	t.Run("should skip refresh when ETag is unchanged", func(t *testing.T) {
		setup()

		sameETag := "etag-1"
		s3Client.On("HeadObject", mock.Anything, "test-bucket", "schemas/event_schema.json").
			Return(&aws.HeadObjectOutput{ETag: &sameETag}, nil)

		validator.On("ETag").Return("etag-1")

		ctx, cancel := context.WithCancel(context.Background())
		suite.StartAutoRefresh(ctx, validator)

		time.Sleep(150 * time.Millisecond)
		cancel()

		validator.AssertNotCalled(t, "Update")
		s3Client.AssertNotCalled(t, "GetObject")
	})
}
