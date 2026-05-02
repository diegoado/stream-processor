package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Producer sends invalid events to the Kafka dead-letter topic.
type Producer struct {
	log      *slog.Logger
	producer kafka.Producer[event.RejectedEvent]
	topic    string
}

// NewProducer creates a DLQ Producer with the given sync Kafka producer and topic.
func NewProducer(producer kafka.Producer[event.RejectedEvent], topic string) *Producer {
	return &Producer{log: logger.Get("dlq-producer"), producer: producer, topic: topic}
}

// Send produces a rejected event envelope to the DLQ topic.
func (p *Producer) Send(ctx context.Context, original event.Event, errors []string) error {
	err := p.producer.Produce(ctx, kafka.ProducerMessage[event.RejectedEvent]{
		Topic: p.topic,
		Value: event.RejectedEvent{
			Event:      original,
			Errors:     errors,
			RejectedAt: time.Now().UTC(),
		},
	})
	if err != nil {
		p.log.Error("failed to produce DLQ message", slog.Any("error", err))
	}
	return err
}

// Close flushes and closes the underlying Kafka producer.
func (p *Producer) Close() {
	p.producer.Flush()
	p.producer.Close()
}
