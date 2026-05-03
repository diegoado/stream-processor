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

| Command              | Description                                              |
|----------------------|----------------------------------------------------------|
| `make help`          | Show all available commands                              |
| `make install`       | Install dependencies, tools, and setup git hooks         |
| `make format`        | Auto-format code                                         |
| `make lint`          | Run linter checks                                        |
| `make test`          | Run unit tests                                           |
| `make integration-test` | Run integration tests (godog + testcontainers)        |
| `make test-all`      | Run unit and integration tests                           |
| `make coverage`      | Run all tests and check minimum coverage (75%)           |
| `make local-up`      | Start local environment (Kafka + LocalStack + services)  |
| `make local-down`    | Stop local environment                                   |

## Key Design Decisions

**Kafka abstraction with generics** — `pkg/kafka` provides `Producer[T]` and `Consumer[T]` interfaces with built-in JSON serialization/deserialization. Internal packages never import `confluent-kafka-go` directly. The producer supports both async (mock producer) and sync (DLQ) modes via a delivery channel.

**AWS client abstraction** — `pkg/aws` wraps S3, SNS, and SQS behind interfaces with simplified signatures, hiding SDK types from business logic. All AWS operations are testable through interface mocks.

**Schema-driven validation and sanitization** — The JSON Schema uses a default + override pattern (`$defs/defaults` and `$defs/overrides`) allowing tenant-specific payload rules without code changes. The `DataSchema` parses the schema to build a payload field index used to strip extra fields before publishing. New tenants are accepted immediately against default rules.

**Interface-driven testability** — All internal components (`Validator`, `Publisher`, `Producer`, `Handler`, `Consumer`, `Loader`, `Processor`) expose interfaces with private concrete implementations. This enables full mock-based unit testing without any infrastructure dependencies.

**Integration tests with testcontainers** — BDD-style tests using godog + testcontainers spin up real Kafka and LocalStack containers. The processor runs as a background goroutine, and tests verify the full pipeline: produce → validate → publish to SNS (verified via SQS) or reject to DLQ (verified via Kafka consumer).

**Configuration per service** — Each service (processor, mock producer, mock sender) has its own config struct and loader. Kafka producer/consumer tuning is configurable via environment variables with production-grade defaults adapted from the startrack-events-processor-v2 Java project.

## Full Specification

See [docs/SPEC.md](docs/SPEC.md) for the complete technical specification.
