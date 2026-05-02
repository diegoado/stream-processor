package config

import "time"

// SchemaConfig holds S3 schema location and refresh settings.
type SchemaConfig struct {
	Bucket          string        `env:"S3_SCHEMA_BUCKET"        envDefault:"stream-processor-schemas"`
	Key             string        `env:"S3_SCHEMA_KEY"           envDefault:"schemas/event_schema.json"`
	RefreshInterval time.Duration `env:"SCHEMA_REFRESH_INTERVAL" envDefault:"5m"`
}
