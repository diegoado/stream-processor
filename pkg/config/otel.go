package config

// OTelConfig holds OpenTelemetry configuration.
type OTelConfig struct {
	Enabled     bool   `env:"OTEL_ENABLED"                envDefault:"false"`
	ServiceName string `env:"OTEL_SERVICE_NAME"           envDefault:"stream-processor"`
	Endpoint    string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://localhost:4318"`
}
