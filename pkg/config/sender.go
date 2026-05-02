package config

import "github.com/caarlos0/env/v10"

// SenderConfig holds mock sender runtime settings.
type SenderConfig struct {
	AWS         AWSConfig
	QueueURL    string `env:"SQS_QUEUE_URL"`
	TenantID    string `env:"TENANT_ID"`
	MaxMessages int32  `env:"SENDER_MAX_MESSAGES" envDefault:"10"`
}

// LoadSenderConfig parses environment variables into a SenderConfig struct.
func LoadSenderConfig() (*SenderConfig, error) {
	cfg := &SenderConfig{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
