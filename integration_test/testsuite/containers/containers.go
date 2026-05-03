//go:build integration

package containers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	kafkaModule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/testcontainers/testcontainers-go/network"
)

const (
	ReadOnly             = 0o644
	ExecutablePermission = 0o755
)

// Infrastructure holds all testcontainer instances.
type Infrastructure struct {
	network    *testcontainers.DockerNetwork
	LocalStack *localstack.LocalStackContainer
	Kafka      *kafkaModule.KafkaContainer
}

// Setup creates the Docker network and starts all containers.
func Setup(ctx context.Context) (*Infrastructure, error) {
	dockerNetwork, err := network.New(ctx, network.WithDriver("bridge"))
	if err != nil {
		return nil, fmt.Errorf("error creating network: %w", err)
	}

	kafka, err := startKafka(ctx, dockerNetwork)
	if err != nil {
		_ = dockerNetwork.Remove(ctx)
		return nil, fmt.Errorf("failed to start kafka: %w", err)
	}

	broker, brokerErr := kafka.Brokers(ctx)
	if brokerErr != nil {
		_ = kafka.Terminate(ctx)
		_ = dockerNetwork.Remove(ctx)
		return nil, fmt.Errorf("failed to get kafka broker: %w", brokerErr)
	}

	if topicErr := createTopics(broker[0], []string{"events", "events.dlq"}); topicErr != nil {
		_ = kafka.Terminate(ctx)
		_ = dockerNetwork.Remove(ctx)
		return nil, fmt.Errorf("failed to create kafka topics: %w", topicErr)
	}

	ls, err := startLocalStack(ctx, dockerNetwork)
	if err != nil {
		_ = kafka.Terminate(ctx)
		_ = dockerNetwork.Remove(ctx)
		return nil, fmt.Errorf("failed to start localstack: %w", err)
	}

	return &Infrastructure{
		network:    dockerNetwork,
		LocalStack: ls,
		Kafka:      kafka,
	}, nil
}

// Teardown terminates all containers and removes the network.
func (i *Infrastructure) Teardown(ctx context.Context) {
	if i.Kafka != nil {
		_ = i.Kafka.Terminate(ctx)
	}
	if i.LocalStack != nil {
		_ = i.LocalStack.Terminate(ctx)
	}
	if i.network != nil {
		_ = i.network.Remove(ctx)
	}
}

// KafkaBroker returns the Kafka broker address.
func (i *Infrastructure) KafkaBroker(ctx context.Context) (string, error) {
	brokers, err := i.Kafka.Brokers(ctx)
	if err != nil {
		return "", err
	}
	return brokers[0], nil
}

// LocalStackEndpoint returns the LocalStack endpoint URL.
func (i *Infrastructure) LocalStackEndpoint(ctx context.Context) (string, error) {
	host, err := i.LocalStack.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := i.LocalStack.MappedPort(ctx, "4566/tcp")
	if err != nil {
		return "", err
	}
	return "http://" + host + ":" + port.Port(), nil
}
