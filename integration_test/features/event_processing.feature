Feature: Event processing
  As the stream processor
  I want to validate and route events to SNS
  So that valid events are published with tenant routing

  Scenario Outline: Valid event is published to SNS
    Given an event with payload:
      """
      {
        "event_id": "<eventId>",
        "event_type": "<eventType>",
        "tenant_id": "<tenantId>",
        "timestamp": "2026-05-01T00:00:00Z",
        "payload": <payload>
      }
      """
    When the event is produced to the "events" topic
    Then the event should be received in the SNS topic

    Examples: Default schemas
      | eventId                              | eventType          | tenantId   | payload                                                        |
      | 550e8400-e29b-41d4-a716-446655440000 | monitoring.alert   | tenant-new | {"severity":"high","message":"CPU alert"}                      |
      | 550e8400-e29b-41d4-a716-446655440001 | monitoring.metric  | tenant-new | {"metric_name":"cpu_usage","value":85.5}                       |
      | 550e8400-e29b-41d4-a716-446655440002 | user.action        | tenant-new | {"action":"click","user_id":"u1"}                              |
      | 550e8400-e29b-41d4-a716-446655440003 | transaction.auth   | tenant-new | {"transaction_id":"tx-1","amount":99.99,"status":"approved"}   |
      | 550e8400-e29b-41d4-a716-446655440004 | webhook.dispatched | tenant-new | {"webhook_id":"wh-1","endpoint":"https://example.com/webhook"} |

    Examples: Tenant-specific overrides
      | eventId                              | eventType          | tenantId | payload                                                                                                     |
      | 550e8400-e29b-41d4-a716-446655440010 | monitoring.alert   | tenant-a | {"severity":"high","source":"cpu-monitor","message":"CPU alert","alert_url":"https://alerts.example.com/1"} |
      | 550e8400-e29b-41d4-a716-446655440011 | transaction.auth   | tenant-a | {"transaction_id":"tx-1","amount":99.99,"currency":"BRL","status":"approved","risk_score":0.42}             |
      | 550e8400-e29b-41d4-a716-446655440012 | user.action        | tenant-b | {"action":"click","user_id":"u1","resource":"/dashboard","session_id":"s1"}                                 |
      | 550e8400-e29b-41d4-a716-446655440013 | webhook.dispatched | tenant-c | {"webhook_id":"wh-1","endpoint":"https://example.com/webhook","http_status":200,"callback_id":"cb-1"}       |

  Scenario: Event with extra payload fields is sanitized before publishing
    Given an event with payload:
      """
      {
        "event_id": "550e8400-e29b-41d4-a716-446655440020",
        "event_type": "monitoring.alert",
        "tenant_id": "tenant-a",
        "timestamp": "2026-05-01T00:00:00Z",
        "payload": {"severity": "high", "message": "alert", "source": "cpu", "unexpected_field": "stripped"}
      }
      """
    When the event is produced to the "events" topic
    Then the event should be received in the SNS topic
