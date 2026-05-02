package utilities

import "github.com/diegoado/stream-processor/pkg/event"

// DoSanitizeEventPayload returns a new event with only the allowed payload fields.
func DoSanitizeEventPayload(e *event.Event, allowedFields map[string]struct{}) *event.Event {
	payload := make(map[string]any, len(allowedFields))
	for k, v := range e.Payload {
		if _, ok := allowedFields[k]; ok {
			payload[k] = v
		}
	}
	return &event.Event{
		EventID:   e.EventID,
		EventType: e.EventType,
		TenantID:  e.TenantID,
		Timestamp: e.Timestamp,
		Payload:   payload,
	}
}
