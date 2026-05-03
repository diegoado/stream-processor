package kafka

import (
	"encoding/json"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/jurabek/otelkafka"

	"github.com/diegoado/stream-processor/pkg/config"
)

// Message represents a consumed Kafka message with a typed deserialized value.
type Message[T any] struct {
	Value T
}

// Consumer abstracts Kafka consumer operations for testability.
type Consumer[T any] interface {
	Subscribe(topic string) error
	Poll(timeout time.Duration) (*Message[T], error)
	Commit() error
	Close() error
}

type consumerImpl[T any] struct {
	consumer *otelkafka.Consumer
	lastMsg  *confluent.Message
}

// NewConsumer creates a production Kafka Consumer from configuration.
func NewConsumer[T any](cfg config.KafkaConfig) (Consumer[T], error) {
	c, err := otelkafka.NewConsumer(&confluent.ConfigMap{
		"bootstrap.servers":         cfg.BootstrapServers,
		"group.id":                  cfg.GroupID,
		"auto.offset.reset":         cfg.Consumer.AutoOffsetReset,
		"enable.auto.commit":        false,
		"max.poll.interval.ms":      cfg.Consumer.MaxPollIntervalMs,
		"session.timeout.ms":        cfg.Consumer.SessionTimeoutMs,
		"heartbeat.interval.ms":     cfg.Consumer.HeartbeatIntervalMs,
		"fetch.min.bytes":           cfg.Consumer.FetchMinBytes,
		"fetch.max.bytes":           cfg.Consumer.FetchMaxBytes,
		"fetch.wait.max.ms":         cfg.Consumer.FetchMaxWaitMs,
		"max.partition.fetch.bytes": cfg.Consumer.MaxPartitionFetchBytes,
	})
	if err != nil {
		return nil, err
	}
	return &consumerImpl[T]{consumer: c}, nil
}

func (c *consumerImpl[T]) Subscribe(topic string) error {
	return c.consumer.Subscribe(topic, nil)
}

func (c *consumerImpl[T]) Poll(timeout time.Duration) (*Message[T], error) {
	ev := c.consumer.Poll(int(timeout.Milliseconds()))
	if ev == nil {
		return nil, nil //nolint:nilnil // no message available is not an error
	}

	switch e := ev.(type) {
	case *confluent.Message:
		c.lastMsg = e

		var value T
		if err := json.Unmarshal(e.Value, &value); err != nil {
			return nil, err
		}
		return &Message[T]{Value: value}, nil
	case confluent.Error:
		return nil, e
	default:
		return nil, nil //nolint:nilnil // non-message events are ignored
	}
}

// Commit commits the offset of the last polled message.
func (c *consumerImpl[T]) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	_, err := c.consumer.CommitMessage(c.lastMsg)
	return err
}

func (c *consumerImpl[T]) Close() error {
	return c.consumer.Close()
}
