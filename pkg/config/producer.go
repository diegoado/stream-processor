package config

import (
	"time"

	"github.com/caarlos0/env/v10"
)

// ProducerConfig holds mock producer runtime settings.
type ProducerConfig struct {
	Kafka                 KafkaConfig
	EventsTopic           string        `env:"PRODUCER_EVENTS_TOPIC"            envDefault:"events"`
	Interval              time.Duration `env:"PRODUCER_INTERVAL"                envDefault:"1s"`
	InvalidEventFrequency int           `env:"PRODUCER_INVALID_EVENT_FREQUENCY" envDefault:"5"`
	ExtraFieldFrequency   int           `env:"PRODUCER_EXTRA_FIELD_FREQUENCY"   envDefault:"3"`
}

// LoadProducerConfig parses environment variables into a ProducerConfig struct.
func LoadProducerConfig() (*ProducerConfig, error) {
	cfg := &ProducerConfig{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
