package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/diegoado/stream-processor/internal/handler"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Consumer polls Kafka for events and delegates processing to the handler.
type Consumer interface {
	Start(ctx context.Context) error
}

type consumerImpl struct {
	log         *slog.Logger
	consumer    kafka.Consumer[event.Event]
	handler     handler.Handler
	topic       string
	pollTimeout time.Duration
}

// NewConsumer creates a Consumer with the given Kafka consumer, handler, and configuration.
func NewConsumer(
	consumer kafka.Consumer[event.Event],
	handler handler.Handler,
	eventsTopic string,
	pollTimeout time.Duration,
) Consumer {
	return &consumerImpl{
		log:         logger.Get("processor-consumer"),
		consumer:    consumer,
		handler:     handler,
		topic:       eventsTopic,
		pollTimeout: pollTimeout,
	}
}

// Start subscribes to the topic and polls in a loop until the context is cancelled.
func (c *consumerImpl) Start(ctx context.Context) error {
	if err := c.consumer.Subscribe(c.topic); err != nil {
		return err
	}
	c.log.Info("consumer started", slog.String("topic", c.topic))

	for {
		select {
		case <-ctx.Done():
			c.log.Info("consumer stopping")
			return c.consumer.Close()
		default:
			if err := c.poll(ctx); err != nil {
				c.log.Error("poll error", slog.Any("error", err))
			}
		}
	}
}

func (c *consumerImpl) poll(ctx context.Context) error {
	msg, err := c.consumer.Poll(c.pollTimeout)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}

	if handleErr := c.handler.Handle(ctx, msg.Value); handleErr != nil {
		return handleErr
	}
	return c.consumer.Commit()
}
