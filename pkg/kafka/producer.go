package kafka

import (
	"context"
	"encoding/json"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"

	"github.com/diegoado/stream-processor/pkg/config"
)

// MessageKey represents the partition key for a Kafka message.
type MessageKey struct {
	TenantID  string `json:"tenant_id"`
	EventType string `json:"event_type"`
}

// ProducerMessage represents a typed message to be produced to a Kafka topic.
type ProducerMessage[T any] struct {
	Topic string
	Key   *MessageKey
	Value T
}

// Producer abstracts Kafka producer operations for testability.
type Producer[T any] interface {
	Produce(ctx context.Context, msg ProducerMessage[T]) error
	Flush()
	Close()
}

type producerImpl[T any] struct {
	producer        *confluent.Producer
	deliveryChannel chan confluent.Event
	flushTimeout    int
}

func newProducer[T any](cfg config.KafkaConfig, ch chan confluent.Event) (Producer[T], error) {
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers":          cfg.BootstrapServers,
		"acks":                       cfg.Producer.Acks,
		"compression.type":           cfg.Producer.CompressionType,
		"linger.ms":                  cfg.Producer.LingerMs,
		"batch.size":                 cfg.Producer.BatchSize,
		"queue.buffering.max.kbytes": cfg.Producer.BufferMaxKbytes,
	})
	if err != nil {
		return nil, err
	}

	return &producerImpl[T]{
		producer:        p,
		deliveryChannel: ch,
		flushTimeout:    int(cfg.Producer.FlushTimeout.Milliseconds()),
	}, nil
}

// NewProducer creates an async Kafka Producer (fire-and-forget).
func NewProducer[T any](cfg config.KafkaConfig) (Producer[T], error) {
	return newProducer[T](cfg, nil)
}

// NewSyncProducer creates a sync Kafka Producer that waits for broker acknowledgment.
func NewSyncProducer[T any](cfg config.KafkaConfig) (Producer[T], error) {
	return newProducer[T](cfg, make(chan confluent.Event))
}

func (p *producerImpl[T]) Produce(_ context.Context, msg ProducerMessage[T]) error {
	value, err := json.Marshal(msg.Value)
	if err != nil {
		return err
	}

	km := &confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &msg.Topic, Partition: confluent.PartitionAny},
		Value:          value,
	}

	if msg.Key != nil {
		keyBytes, marshalErr := json.Marshal(msg.Key)
		if marshalErr != nil {
			return marshalErr
		}
		km.Key = keyBytes
	}

	if produceErr := p.producer.Produce(km, p.deliveryChannel); produceErr != nil {
		return produceErr
	}

	if p.deliveryChannel != nil {
		e := <-p.deliveryChannel
		m, ok := e.(*confluent.Message)
		if ok && m.TopicPartition.Error != nil {
			return errors.Errorf("delivery failed: %s", m.TopicPartition.Error)
		}
	}
	return nil
}

func (p *producerImpl[T]) Flush() {
	p.producer.Flush(p.flushTimeout)
}

func (p *producerImpl[T]) Close() {
	if p.deliveryChannel != nil {
		close(p.deliveryChannel)
	}
	p.producer.Close()
}
