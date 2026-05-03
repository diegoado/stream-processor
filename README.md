# Stream Processor

A reactive Go service that consumes events from Kafka, validates them against a JSON Schema loaded from S3, and routes valid events to tenant-specific SQS queues via SNS. Invalid events are sent to a Kafka dead-letter topic with error descriptions.

## Architecture

```
                                ┌──────────────────────┐
                                │   S3 (JSON Schema)   │
                                │  auto-refresh 5min   │
                                └──────────┬───────────┘
                                           │
[Producers] ──► [Kafka: events] ──► [Stream Processor] ──► [SNS Topic]
                                       │                       │
                                       │               ┌───────┴──────┐
                                       ▼               ▼              ▼
                              [Kafka: events.dlq]  [SQS: tenant-a] [SQS: tenant-b]
                              (invalid + reason)       │             │
                                                       ▼             ▼
                                                   [Sender A]   [Sender B]
                                                   (log events) (log events)
```

## Make Commands

| Command                   | Description                                             |
|---------------------------|---------------------------------------------------------|
| `make help`               | Show all available commands                             |
| `make install`            | Install dependencies, tools, and setup git hooks        |
| `make format`             | Auto-format code                                        |
| `make lint`               | Run linter checks                                       |
| `make test`               | Run unit tests                                          |
| `make integration-test`   | Run integration tests (godog + testcontainers)          |
| `make test-all`           | Run unit and integration tests                          |
| `make coverage`           | Run all tests and check minimum coverage (75%)          |
| `make mutation-test`      | Run mutation tests (gremlins)                           |
| `make check-mutants`      | Run mutation tests and check minimum score (60%)        |
| `make local-up`           | Start local environment (Kafka + LocalStack + services) |
| `make local-down`         | Stop local environment                                  |
| `make otel-up`            | Start local environment with OpenTelemetry + Grafana    |
| `make otel-down`          | Stop local environment with OpenTelemetry + Grafana     |

## Key Design Decisions

**Kafka abstraction with generics** — `pkg/kafka` provides `Producer[T]` and `Consumer[T]` interfaces with built-in JSON serialization/deserialization. Internal packages never import `confluent-kafka-go` directly. The producer supports both async (mock producer) and sync (DLQ) modes via a delivery channel.

**AWS client abstraction** — `pkg/aws` wraps S3, SNS, and SQS behind interfaces with simplified signatures, hiding SDK types from business logic. All AWS operations are testable through interface mocks.

**Schema-driven validation and sanitization** — The JSON Schema uses a default + override pattern (`$defs/defaults` and `$defs/overrides`) allowing tenant-specific payload rules without code changes. The `DataSchema` parses the schema to build a payload field index used to strip extra fields before publishing. New tenants are accepted immediately against default rules.

**Interface-driven testability** — All internal components (`Validator`, `Publisher`, `Producer`, `Handler`, `Consumer`, `Loader`, `Processor`) expose interfaces with private concrete implementations. This enables full mock-based unit testing without any infrastructure dependencies.

**Integration tests with testcontainers** — BDD-style tests using godog + testcontainers spin up real Kafka and LocalStack containers. The processor runs as a background goroutine, and tests verify the full pipeline: produce → validate → publish to SNS (verified via SQS) or reject to DLQ (verified via Kafka consumer).

**Configuration per service** — Each service (processor, mock producer, mock sender) has its own config struct and loader. Kafka producer/consumer tuning is configurable via environment variables with production-grade.

**OpenTelemetry observability** — Traces, metrics, and logs exported via OTLP HTTP. Kafka consumer/producer instrumented via `otelkafka`, AWS clients via `otelaws`. Logs bridged from `slog` to OTel via `otelslog` composite handler (stdout + OTel). `make otel-up` starts the full Grafana stack: OTel Collector → Tempo (traces), Mimir (metrics), Loki (logs) → Grafana (dashboards at `localhost:3000`). Disabled by default (`OTEL_ENABLED=false`), zero overhead when off.

## Full Specification

See [docs/SPEC.md](docs/SPEC.md) for the complete technical specification.

## Next Features

**Event redelivery via DynamoDB** — Currently, once an event is delivered to a tenant SQS queue and consumed by a sender, there is no way to redeliver it. The proposed solution adds an async DynamoDB write directly from the processor, keeping events available for up to 30 days (TTL) for on-demand redelivery without replaying from Kafka.

```
[Stream Processor] ──► [SNS Topic] ──► [SQS: tenant-a]    (real-time delivery)
         │
         └──────────► [DynamoDB: events]                  (async, non-blocking)
                            │
           [Redelivery API] ┘ ──► [SQS: tenant-a]         (on-demand redelivery)
```

- The processor writes each valid event to a DynamoDB table asynchronously after publishing to SNS — failures are logged without blocking the main flow
- Table design: partition key `tenant_id`, sort key `timestamp#event_id`, GSI on `event_id` for single-event lookups
- DynamoDB TTL automatically expires events after 30 days
- A redelivery API (or CLI) queries by `event_id`, `tenant_id`, or time range and republishes matching events directly to the target tenant SQS queue
