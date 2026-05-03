//go:build integration

package containers

import (
	"context"
	"fmt"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

func startKafka(ctx context.Context, nw *testcontainers.DockerNetwork) (*kafka.KafkaContainer, error) {
	requestOption := testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {"kafka"}},
		},
	})
	return kafka.Run(ctx,
		"confluentinc/confluent-local:7.9.6",
		kafka.WithClusterID("test-cluster"),
		requestOption,
	)
}

func createTopics(broker string, topics []string) error {
	admin, err := confluent.NewAdminClient(&confluent.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	specs := make([]confluent.TopicSpecification, len(topics))
	for i, topic := range topics {
		specs[i] = confluent.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = admin.CreateTopics(ctx, specs)
	return err
}
