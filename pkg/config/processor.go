package config

import "time"

// ProcessorConfig holds stream processor runtime settings.
type ProcessorConfig struct {
	EventsTopic         string        `env:"PROCESSOR_EVENTS_TOPIC"`
	AcceptedEventsTopic string        `env:"PROCESSOR_ACCEPTED_EVENTS_TOPIC"`
	RejectedEventsTopic string        `env:"PROCESSOR_REJECTED_EVENTS_TOPIC"`
	PollTimeout         time.Duration `env:"PROCESSOR_POLL_TIMEOUT"          envDefault:"100ms"`
}
