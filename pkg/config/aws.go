package config

// AWSConfig holds AWS region and endpoint settings.
type AWSConfig struct {
	Endpoint string `env:"AWS_ENDPOINT" envDefault:"http://localhost:4566"`
	Region   string `env:"AWS_REGION"   envDefault:"us-east-1"`
}
