package aws

import (
	"context"
	"encoding/json"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"

	"github.com/diegoado/stream-processor/pkg/config"
)

// SNSClient abstracts SNS operations for testability.
type SNSClient interface {
	Publish(ctx context.Context, topicARN string, message any, attributes map[string]string) error
}

type snsClientImpl struct {
	client *sns.Client
}

// NewSNSClient creates a production SNS client from AWS configuration.
func NewSNSClient(ctx context.Context, cfg config.AWSConfig) (SNSClient, error) {
	snsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsCfg.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	otelaws.AppendMiddlewares(&snsCfg.APIOptions)

	client := sns.NewFromConfig(snsCfg, func(o *sns.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = awsSDK.String(cfg.Endpoint)
		}
	})
	return &snsClientImpl{client: client}, nil
}

func (c *snsClientImpl) Publish(ctx context.Context, topicARN string, msg any, attributes map[string]string) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	message := string(data)
	msgAttrs := make(map[string]types.MessageAttributeValue, len(attributes))
	dataType := "String"
	for k, v := range attributes {
		val := v
		msgAttrs[k] = types.MessageAttributeValue{DataType: &dataType, StringValue: &val}
	}

	_, err = c.client.Publish(ctx, &sns.PublishInput{
		TopicArn:          &topicARN,
		Message:           &message,
		MessageAttributes: msgAttrs,
	})
	return err
}
