package publisher

import (
	"context"
	"log/slog"

	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Publisher publishes valid events to an SNS topic with tenant routing.
type Publisher struct {
	log      *slog.Logger
	client   aws.SNSClient
	topicARN string
}

// NewPublisher creates a Publisher with the given SNS client and topic ARN.
func NewPublisher(client aws.SNSClient, topicARN string) *Publisher {
	return &Publisher{log: logger.Get("sns-publisher"), client: client, topicARN: topicARN}
}

// Publish sends an event to SNS with tenant_id as a message attribute.
func (p *Publisher) Publish(ctx context.Context, evt *event.Event) error {
	err := p.client.Publish(ctx, p.topicARN, evt, map[string]string{
		"tenant_id": evt.TenantID,
	})
	if err != nil {
		p.log.Error("failed to publish event", slog.String("tenant_id", evt.TenantID), slog.Any("error", err))
	}
	return err
}
