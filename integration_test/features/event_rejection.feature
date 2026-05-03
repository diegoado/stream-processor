Feature: Event rejection
  As the stream processor
  I want to reject invalid events to a dead-letter queue
  So that malformed events are flagged without blocking processing

  Scenario: Event missing required field is rejected
    Given an event with payload:
      """
      {
        "event_id": "550e8400-e29b-41d4-a716-446655440014",
        "tenant_id": "tenant-a",
        "timestamp": "2026-05-01T00:00:00Z",
        "payload": {"severity": "high", "message": "alert"}
      }
      """
    When the event is produced to the "events" topic
    Then the event should appear in the "events.dlq" topic
    And the DLQ message should contain error "event_type"

  Scenario: Event with invalid timestamp format is rejected
    Given an event with payload:
      """
      {
        "event_id": "550e8400-e29b-41d4-a716-446655440015",
        "event_type": "monitoring.alert",
        "tenant_id": "tenant-a",
        "timestamp": "not-a-date",
        "payload": {"severity": "high", "message": "alert"}
      }
      """
    When the event is produced to the "events" topic
    Then the event should appear in the "events.dlq" topic
    And the DLQ message should contain error "date-time"

  Scenario: Event with empty payload is rejected
    Given an event with payload:
      """
      {
        "event_id": "550e8400-e29b-41d4-a716-446655440016",
        "event_type": "monitoring.alert",
        "tenant_id": "tenant-a",
        "timestamp": "2026-05-01T00:00:00Z",
        "payload": {}
      }
      """
    When the event is produced to the "events" topic
    Then the event should appear in the "events.dlq" topic
    And the DLQ message should contain error "required"
