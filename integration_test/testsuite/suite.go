//go:build integration

package testsuite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/diegoado/stream-processor/pkg/event"
)

// SuiteClient provides test clients for Kafka and AWS services.
type SuiteClient struct {
	KafkaProducer *kafka.Producer
	KafkaConsumer *kafka.Consumer
	S3            *s3.Client
	SQS           *sqs.Client
	testQueueURL  string
}

// NewSuiteClient creates test clients from environment variables.
func NewSuiteClient() *SuiteClient {
	ctx := context.Background()
	kafkaBroker := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	awsEndpoint := os.Getenv("AWS_ENDPOINT")

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
	})
	if err != nil {
		panic(fmt.Errorf("failed to create kafka producer: %w", err))
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaBroker,
		"group.id":           fmt.Sprintf("integration-test-%d", time.Now().UnixNano()),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		panic(fmt.Errorf("failed to create kafka consumer: %w", err))
	}

	cfg, err := awsCfg.LoadDefaultConfig(ctx,
		awsCfg.WithRegion("us-east-1"),
		awsCfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		panic(fmt.Errorf("failed to load aws config: %w", err))
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
		o.UsePathStyle = true
	})

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
	})

	return &SuiteClient{
		KafkaProducer: producer,
		KafkaConsumer: consumer,
		S3:            s3Client,
		SQS:           sqsClient,
		testQueueURL:  awsEndpoint + "/000000000000/test-queue",
	}
}

// ProduceEvent sends a JSON event to the given Kafka topic.
func (c *SuiteClient) ProduceEvent(topic string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return c.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)
}

// ReceiveSNSMessage polls the test SQS queue for an SNS message matching the given eventID.
func (c *SuiteClient) ReceiveSNSMessage(eventID string, timeout time.Duration) (*event.Event, error) {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		out, err := c.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &c.testQueueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
			VisibilityTimeout:   0,
		})
		if err != nil {
			return nil, err
		}
		for _, sqsMsg := range out.Messages {
			var msg struct {
				Message string `json:"Message"`
			}
			if err = json.Unmarshal([]byte(*sqsMsg.Body), &msg); err != nil {
				continue
			}
			var evt event.Event
			if err = json.Unmarshal([]byte(msg.Message), &evt); err != nil {
				continue
			}
			if evt.EventID == eventID {
				return &evt, nil
			}
		}
	}
	return nil, fmt.Errorf("no SNS message with event_id %q received within %s", eventID, timeout)
}

// ConsumeDlqMessage polls the DLQ Kafka topic for a message matching the given eventID.
func (c *SuiteClient) ConsumeDlqMessage(topic, eventID string, timeout time.Duration) (*event.RejectedEvent, error) {
	if err := c.KafkaConsumer.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ev := c.KafkaConsumer.Poll(int(time.Until(deadline).Milliseconds()))
		if ev == nil {
			continue
		}
		msg, ok := ev.(*kafka.Message)
		if !ok {
			continue
		}
		var rejected event.RejectedEvent
		if err := json.Unmarshal(msg.Value, &rejected); err != nil {
			continue
		}
		if rejected.Event.EventID == eventID {
			return &rejected, nil
		}
	}
	return nil, fmt.Errorf("no DLQ message with event_id %q received within %s", eventID, timeout)
}

// Close cleans up test clients.
func (c *SuiteClient) Close() {
	c.KafkaProducer.Close()
	_ = c.KafkaConsumer.Close()
}
