//go:build sender

package aws

import (
	"context"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/diegoado/stream-processor/pkg/config"
)

// SQSMessage represents a message received from an SQS queue.
type SQSMessage struct {
	Body          string
	ReceiptHandle string
}

// SQSClient abstracts SQS operations for testability.
type SQSClient interface {
	ReceiveMessages(ctx context.Context, queueURL string, maxMessages int32) ([]SQSMessage, error)
	DeleteMessage(ctx context.Context, queueURL, receiptHandle string) error
}

type sqsClientImpl struct {
	client *sqs.Client
}

// NewSQSClient creates a production SQS client from AWS configuration.
func NewSQSClient(ctx context.Context, cfg config.AWSConfig) (SQSClient, error) {
	sqsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsCfg.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	client := sqs.NewFromConfig(sqsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = awsSDK.String(cfg.Endpoint)
		}
	})
	return &sqsClientImpl{client: client}, nil
}

func (c *sqsClientImpl) ReceiveMessages(
	ctx context.Context,
	queueURL string,
	maxMessages int32,
) ([]SQSMessage, error) {
	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     10, //nolint:mnd // long-poll 10s
	})
	if err != nil {
		return nil, err
	}

	messages := make([]SQSMessage, len(out.Messages))
	for i, m := range out.Messages {
		messages[i] = SQSMessage{
			Body:          *m.Body,
			ReceiptHandle: *m.ReceiptHandle,
		}
	}
	return messages, nil
}

func (c *sqsClientImpl) DeleteMessage(
	ctx context.Context,
	queueURL, receiptHandle string,
) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: &receiptHandle,
	})
	return err
}
