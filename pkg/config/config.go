package config

import "github.com/caarlos0/env/v10"

// Config aggregates all application configuration sections.
type Config struct {
	Kafka     KafkaConfig
	AWS       AWSConfig
	Schema    SchemaConfig
	Processor ProcessorConfig
}

// Load parses environment variables into a Config struct.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
