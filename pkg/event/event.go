package event

import "time"

// Event represents an incoming event from the Kafka topic.
type Event struct {
	EventID   string         `json:"event_id"`
	EventType string         `json:"event_type"`
	TenantID  string         `json:"tenant_id"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// RejectedEvent is the DLQ envelope wrapping an invalid event with error details.
type RejectedEvent struct {
	Event      Event     `json:"original_event"`
	Errors     []string  `json:"errors"`
	RejectedAt time.Time `json:"rejected_at"`
}
