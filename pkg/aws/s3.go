package aws

import (
	"context"
	"io"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"

	"github.com/diegoado/stream-processor/pkg/config"
)

// HeadObjectOutput holds metadata from an S3 HeadObject call.
type HeadObjectOutput struct {
	ETag *string
}

// GetObjectOutput holds the body and metadata from an S3 GetObject call.
type GetObjectOutput struct {
	Body io.ReadCloser
	ETag *string
}

// S3Client abstracts S3 operations for testability.
type S3Client interface {
	HeadObject(ctx context.Context, bucket, key string) (*HeadObjectOutput, error)
	GetObject(ctx context.Context, bucket, key string) (*GetObjectOutput, error)
}

type s3ClientImpl struct {
	client *s3.Client
}

// NewS3Client creates a production S3Client from AWS configuration.
func NewS3Client(ctx context.Context, cfg config.AWSConfig) (S3Client, error) {
	s3Cfg, err := awsCfg.LoadDefaultConfig(ctx, awsCfg.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	otelaws.AppendMiddlewares(&s3Cfg.APIOptions)

	client := s3.NewFromConfig(s3Cfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = awsSDK.String(cfg.Endpoint)
		}
		o.UsePathStyle = true
	})
	return &s3ClientImpl{client: client}, nil
}

func (c *s3ClientImpl) HeadObject(ctx context.Context, bucket, key string) (*HeadObjectOutput, error) {
	out, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return &HeadObjectOutput{ETag: out.ETag}, nil
}

func (c *s3ClientImpl) GetObject(ctx context.Context, bucket, key string) (*GetObjectOutput, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	return &GetObjectOutput{Body: out.Body, ETag: out.ETag}, nil
}
