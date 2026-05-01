# Stream Processor — Technical Specification

## 1. Overview

The **stream-processor** is a reactive Go service that consumes events from a Kafka topic, validates them against a JSON Schema loaded from S3 (auto-refreshed every 5 minutes), and routes valid events to tenant-specific SQS queues via SNS. Invalid events are sent to a Kafka dead-letter topic with error descriptions.

```
                                ┌──────────────────────┐
                                │   S3 (JSON Schema)   │
                                │  auto-refresh 5min   │
                                └──────────┬───────────┘
                                           │
[Producers] ──► [Kafka: events] ──► [Stream Processor] ──► [SNS Topic]
                                       │                       │
                                       │               ┌──────┴──────┐
                                       ▼               ▼             ▼
                              [Kafka: events.dlq]  [SQS: tenant-a] [SQS: tenant-b]
                              (invalid + reason)       │             │
                                                       ▼             ▼
                                                   [Sender A]   [Sender B]
                                                   (log events) (log events)
```

## 2. Components

### 2.1 Stream Processor (core service)

- **Consumes** from Kafka topic `events` using a consumer group
- **Validates** each event against a JSON Schema fetched from S3
- **Publishes** valid events to an SNS topic with `tenant_id` as a message attribute
- **Produces** invalid events to Kafka topic `events.dlq` with the original payload + validation errors
- **Schema refresh**: background goroutine polls S3 every 5 minutes, reloads schema on change (ETag comparison)

### 2.2 Mock Producer

- CLI tool that continuously produces events to the `events` Kafka topic
- Generates events for multiple tenants (`tenant-a`, `tenant-b`, `tenant-c`)
- Periodically produces intentionally invalid events (missing required fields, wrong types)

### 2.3 Mock Sender

- Lightweight service that polls a specific SQS queue and logs received events
- One instance per tenant (configured via env var)
- Demonstrates the end-to-end flow: producer → processor → SNS → SQS → sender

## 3. Event Contract

### 3.1 Event Schema (JSON Schema Draft-07)

Stored in S3, validated using `xeipuuv/gojsonschema` (same lib as startrack-events-receiver):

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "event_id":     { "type": "string", "format": "uuid" },
    "event_type":   { "type": "string" },
    "tenant_id":    { "type": "string" },
    "timestamp":    { "type": "string", "format": "date-time" },
    "payload":      { "type": "object" }
  },
  "required": ["event_id", "event_type", "tenant_id", "timestamp", "payload"],
  "additionalProperties": false
}
```

| Field        | Type   | Description                                                              |
|-------------|--------|--------------------------------------------------------------------------|
| `event_id`  | string | UUID v4 identifying the event                                            |
| `event_type`| string | Producer context (e.g., `monitoring.alert`, `user.action`)               |
| `tenant_id` | string | Client/tenant identifier — used as SNS message attribute for SQS routing |
| `timestamp` | string | ISO 8601 datetime when the event was created                             |
| `payload`   | object | Event-specific data, opaque to the processor                             |

### 3.2 DLQ Message Format

Invalid events are produced to `events.dlq`:

```json
{
  "original_event": { ... },
  "errors": ["event_id is required", "timestamp: Does not match format 'date-time'"],
  "rejected_at": "2026-05-01T00:00:00Z"
}
```

## 4. Architecture Decisions

### 4.1 Kafka — `confluentinc/confluent-kafka-go/v2` v2.12.0

Same library used in startrack-events-receiver. No Schema Registry — plain JSON.

- **Consumer**: consumer group on `events` topic, manual offset commit after processing
- **DLQ Producer**: writes rejected events to `events.dlq` with error envelope
- Consumer group ensures resilience: if the processor crashes, another instance resumes from last committed offset

### 4.2 JSON Schema Validation — `xeipuuv/gojsonschema` v1.2.0

Same library and `BodyValidator` pattern from startrack-events-receiver. Differences:

- Schema loaded from S3 instead of `//go:embed`
- Background goroutine polls S3 every 5 minutes using `HeadObject` (ETag comparison)
- Downloads via `GetObject` only when ETag changes
- Thread-safe schema swap using `sync.RWMutex`

### 4.3 SNS + SQS Fan-out (LocalStack)

- One SNS topic: `events-topic`
- Per-tenant SQS queues: `tenant-a-queue`, `tenant-b-queue`, `tenant-c-queue`
- SNS subscription filter policy on message attribute `tenant_id` routes to the correct queue
- LocalStack provides SNS, SQS, S3 locally

### 4.4 Configuration — `caarlos0/env/v10`

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=stream-processor
KAFKA_EVENTS_TOPIC=events
KAFKA_DLQ_TOPIC=events.dlq

SNS_TOPIC_ARN=arn:aws:sns:us-east-1:000000000000:events-topic

S3_SCHEMA_BUCKET=stream-processor-schemas
S3_SCHEMA_KEY=schemas/event_schema.json
SCHEMA_REFRESH_INTERVAL=5m

AWS_ENDPOINT=http://localhost:4566
AWS_REGION=us-east-1
```

## 5. Project Structure

```
stream-processor/
├── cmd/
│   ├── processor/main.go          # Stream processor entry point
│   ├── producer/main.go           # Mock producer entry point
│   └── sender/main.go             # Mock sender entry point
├── internal/
│   ├── consumer/
│   │   ├── consumer.go            # Kafka consumer loop + graceful shutdown
│   │   └── consumer_test.go
│   ├── handler/
│   │   ├── handler.go             # Validate → publish or reject
│   │   └── handler_test.go
│   ├── schema/
│   │   ├── loader.go              # S3 schema loader with auto-refresh
│   │   └── loader_test.go
│   ├── publisher/
│   │   ├── sns.go                 # SNS publisher (valid events)
│   │   └── sns_test.go
│   └── dlq/
│       ├── producer.go            # Kafka DLQ producer (invalid events)
│       └── producer_test.go
├── pkg/
│   ├── config/config.go           # Env-based config structs
│   ├── event/event.go             # Event types + DLQ envelope
│   └── logger/logger.go           # slog wrapper (JSON prod / text dev)
├── schemas/
│   └── event_schema.json          # JSON Schema (uploaded to S3 by init script)
├── godog/
│   ├── go.mod                     # Separate module (like tracking-partition-manager)
│   ├── entrypoint_integration_test.go  # TestMain: setup infra, init suite, teardown
│   ├── cucumber_integration_test.go    # TestXxx per feature file
│   ├── features/
│   │   ├── event_processing.feature    # Valid event → SNS → SQS scenarios
│   │   └── event_rejection.feature     # Invalid event → DLQ scenarios
│   ├── steps/
│   │   ├── common.go                   # Shared step defs + ScenarioCtx
│   │   ├── processing.go              # Valid event processing steps
│   │   └── rejection.go               # Invalid event rejection steps
│   └── testsuite/
│       ├── suite.go                    # SuiteClient (Kafka, SQS, S3 test clients)
│       ├── infra.go                    # Init: set env vars from container endpoints
│       ├── containers/
│       │   ├── containers.go           # Infrastructure struct + Setup/Teardown
│       │   ├── kafka.go                # Kafka + Zookeeper testcontainers
│       │   └── localstack.go           # LocalStack testcontainer (SNS, SQS, S3)
│       └── helpers/
│           └── event_builder.go        # Test event fixtures
├── scripts/
│   ├── common.sh                  # Shared variables and helpers
│   ├── setup-hooks.sh             # Git hooks installer (pure shell, no JS)
│   ├── lint.sh
│   ├── test.sh
│   ├── coverage.sh
│   ├── integration-test.sh
│   ├── format.sh
│   └── install.sh
├── docker-compose.yml             # Kafka + LocalStack + producer + senders
├── localstack/
│   └── init-aws.sh                # Creates SNS, SQS, S3, subscriptions, uploads schema
├── Makefile
├── .golangci.yml
├── .editorconfig
├── .gitignore
├── go.mod
└── go.sum
```

## 6. Build & Development

### 6.1 Makefile

Follows tracking-partition-manager pattern — all targets delegate to `scripts/`:

```makefile
.PHONY: help install format lint test coverage integration-test ci local-up local-down

help:             ## Show command list
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:          ## Install dependencies and tools, setup git hooks
	@./scripts/install.sh

format:           ## Auto-format code
	@./scripts/format.sh

lint:             ## Run linter checks
	@./scripts/lint.sh

test:             ## Run unit tests
	@./scripts/test.sh $(ARGS)

coverage:         ## Run tests and check minimum coverage
	@./scripts/coverage.sh $(ARGS)

integration-test: ## Run integration tests (godog + testcontainers)
	@./scripts/integration-test.sh $(ARGS)

ci: install lint coverage ## Execute all project checks

local-up:         ## Start docker-compose (Kafka + LocalStack + mock services)
	docker compose up -d

local-down:       ## Stop docker-compose
	docker compose down
```

### 6.2 Git Hooks (Go-only, no JavaScript)

Pure shell approach — no husky, no commitlint, no node_modules:

**`scripts/setup-hooks.sh`** — called by `make install`:
- Copies hook scripts to `.git/hooks/`
- Makes them executable

**Hooks:**
- **pre-commit**: runs `./scripts/lint.sh`
- **pre-push**: runs `./scripts/coverage.sh`
- **commit-msg**: shell regex validating conventional commits:
  ```
  ^(feat|fix|docs|style|refactor|test|chore|ci|build|perf|revert)(\(.+\))?(!)?: .{1,}$
  ```

### 6.3 Linting

`.golangci.yml` adapted from tracking-partition-manager:
- `goimports` with local prefix `github.com/olxbr/stream-processor`
- `golines` max 120 chars
- `testpackage` enforcement
- `log/slog` enforcement, no globals
- Relaxed rules for test files

## 7. Testing Strategy

### 7.1 Unit Tests

- **Framework**: `testify` (assert + mock)
- **Pattern**: interface-driven with two constructors (production + test)
- **Coverage minimum**: 79%
- **Test areas**:
  - `handler`: validate → route logic with mocked dependencies
  - `schema/loader`: S3 polling, ETag comparison, thread-safe schema swap
  - `publisher/sns`: message attribute construction, error handling
  - `dlq/producer`: DLQ envelope construction

### 7.2 Integration Tests (godog + testcontainers)

Follows the exact same pattern as tracking-partition-manager.

**Module structure**: separate `godog/` Go module with its own `go.mod`, referencing the main module via `replace` directive.

**Infrastructure** (`godog/testsuite/containers/`):

```go
// containers.go
type Infrastructure struct {
    network    *testcontainers.DockerNetwork
    LocalStack *localstack.LocalStackContainer
    Kafka      testcontainers.Container
    Zookeeper  testcontainers.Container
}

func Setup(ctx context.Context) (*Infrastructure, error) { ... }
func (i *Infrastructure) Teardown(ctx context.Context)    { ... }
func (i *Infrastructure) KafkaBroker(ctx context.Context) (string, error)       { ... }
func (i *Infrastructure) LocalStackEndpoint(ctx context.Context) (string, error) { ... }
```

- Docker network shared between containers
- LocalStack: SNS, SQS, S3 (with init script that creates resources + uploads schema)
- Kafka + Zookeeper: topics `events` and `events.dlq` auto-created

**Entrypoint** (`godog/entrypoint_integration_test.go`):

```go
func TestMain(m *testing.M) {
    ctx := context.Background()
    infra, err := containers.Setup(ctx)
    if err != nil {
        panic(fmt.Errorf("failed to setup infrastructure: %w", err))
    }
    testsuite.Init(infra)
    exitCode := m.Run()
    infra.Teardown(ctx)
    os.Exit(exitCode)
}
```

**Suite client** (`godog/testsuite/suite.go`):

```go
type SuiteClient struct {
    KafkaProducer  *KafkaTestProducer   // produces test events to 'events' topic
    KafkaConsumer  *KafkaTestConsumer   // reads from 'events.dlq' to verify rejections
    SQS            *SQSTestClient       // reads from tenant queues to verify delivery
    S3             *S3TestClient         // uploads/updates schema in S3
}
```

**Feature files** (`godog/features/`):

```gherkin
# event_processing.feature
Feature: Event processing
  As the stream processor
  I want to validate and route events to the correct tenant queue
  So that each tenant receives only their events

  Scenario Outline: Valid event is delivered to the correct tenant queue
    Given a valid event for tenant "<tenant_id>" with type "<event_type>"
    When the event is produced to the events topic
    Then the event should be received in the "<tenant_id>-queue" SQS queue
    And the event should not appear in the DLQ topic

    Examples:
      | tenant_id | event_type        |
      | tenant-a  | monitoring.alert  |
      | tenant-b  | user.action       |
      | tenant-c  | transaction.auth  |

  Scenario: Events from different tenants are routed to their respective queues
    Given valid events for tenants "tenant-a" and "tenant-b"
    When the events are produced to the events topic
    Then tenant-a's event should be in "tenant-a-queue"
    And tenant-b's event should be in "tenant-b-queue"
```

```gherkin
# event_rejection.feature
Feature: Event rejection
  As the stream processor
  I want to reject invalid events to a dead-letter queue
  So that malformed events are flagged without blocking processing

  Scenario: Event missing required field is rejected
    Given an event without the "tenant_id" field
    When the event is produced to the events topic
    Then the event should appear in the DLQ topic
    And the DLQ message should contain error "tenant_id is required"
    And the event should not be in any SQS queue

  Scenario: Event with invalid timestamp format is rejected
    Given an event with timestamp "not-a-date"
    When the event is produced to the events topic
    Then the event should appear in the DLQ topic
    And the DLQ message should contain error "Does not match format 'date-time'"

  Scenario: Schema update is picked up after refresh
    Given a valid event with an extra field "unexpected_field"
    When the event is produced to the events topic
    Then the event should appear in the DLQ topic
    And the DLQ message should contain error "Additional property unexpected_field is not allowed"
```

**Step definitions** (`godog/steps/`):

- `common.go`: `ScenarioCtx` struct holding `*testsuite.SuiteClient`, shared Given/Then steps, `After` hook for cleanup
- `processing.go`: `InitializeProcessingScenario` — steps for valid event flow
- `rejection.go`: `InitializeRejectionScenario` — steps for invalid event flow

**Test runner** (`godog/cucumber_integration_test.go`):

```go
func TestEventProcessing(t *testing.T) {
    suite := godog.TestSuite{
        ScenarioInitializer: steps.InitializeProcessingScenario,
        Options: &godog.Options{
            Format:   "pretty",
            Paths:    []string{"features/event_processing.feature"},
            TestingT: t,
        },
    }
    if suite.Run() != 0 {
        t.Fatal("failed to run event processing feature tests")
    }
}

func TestEventRejection(t *testing.T) {
    suite := godog.TestSuite{
        ScenarioInitializer: steps.InitializeRejectionScenario,
        Options: &godog.Options{
            Format:   "pretty",
            Paths:    []string{"features/event_rejection.feature"},
            TestingT: t,
        },
    }
    if suite.Run() != 0 {
        t.Fatal("failed to run event rejection feature tests")
    }
}
```

**Key godog dependencies** (mirroring tracking-partition-manager):

| Library | Purpose |
|---------|---------|
| `cucumber/godog` v0.15.1 | BDD test framework |
| `testcontainers/testcontainers-go` v0.41.0 | Container lifecycle |
| `testcontainers/testcontainers-go/modules/localstack` v0.41.0 | LocalStack module |
| `rdumont/assistdog` | Table parsing helpers |
| `stretchr/testify` | Assertions |

## 8. Docker Compose (Local Environment)

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.9.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  localstack:
    image: localstack/localstack:4.4.0
    ports: ["4566:4566"]
    environment:
      SERVICES: sns,sqs,s3
      DEFAULT_REGION: us-east-1
    volumes:
      - ./localstack/init-aws.sh:/etc/localstack/init/ready.d/init-aws.sh
      - ./schemas:/tmp/schemas

  processor:
    build: { context: ., dockerfile: Dockerfile, target: processor }
    depends_on: [kafka, localstack]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      AWS_ENDPOINT: http://localstack:4566

  producer:
    build: { context: ., dockerfile: Dockerfile, target: producer }
    depends_on: [kafka]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092

  sender-tenant-a:
    build: { context: ., dockerfile: Dockerfile, target: sender }
    depends_on: [localstack]
    environment:
      SQS_QUEUE_URL: http://localstack:4566/000000000000/tenant-a-queue
      AWS_ENDPOINT: http://localstack:4566
      TENANT_ID: tenant-a

  sender-tenant-b:
    build: { context: ., dockerfile: Dockerfile, target: sender }
    depends_on: [localstack]
    environment:
      SQS_QUEUE_URL: http://localstack:4566/000000000000/tenant-b-queue
      AWS_ENDPOINT: http://localstack:4566
      TENANT_ID: tenant-b
```

### 8.1 LocalStack Init Script (`localstack/init-aws.sh`)

1. Create S3 bucket `stream-processor-schemas`
2. Upload `schemas/event_schema.json` to S3
3. Create SNS topic `events-topic`
4. Create SQS queues: `tenant-a-queue`, `tenant-b-queue`, `tenant-c-queue`
5. Subscribe each queue to SNS with filter policy `{"tenant_id": ["tenant-a"]}` etc.

## 9. Key Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| `confluentinc/confluent-kafka-go/v2` | v2.12.0 | Kafka consumer + DLQ producer |
| `xeipuuv/gojsonschema` | v1.2.0 | JSON Schema validation |
| `aws-sdk-go-v2` | latest | SNS publish, SQS receive, S3 schema loading |
| `caarlos0/env/v10` | v10.0.0 | Environment-based configuration |
| `stretchr/testify` | v1.9.0 | Unit test assertions + mocks |
| `log/slog` (stdlib) | — | Structured logging (JSON prod / text dev) |

## 10. Graceful Shutdown

The processor handles `SIGINT`/`SIGTERM`:
1. Stop Kafka consumer (close consumer, stop polling)
2. Flush DLQ producer (wait for pending deliveries)
3. Cancel schema refresh goroutine
4. Exit cleanly

## 11. Implementation Phases

### Phase 1: Project Scaffolding
- Go module, Makefile, scripts, git hooks, .golangci.yml, .editorconfig, .gitignore
- Logger, config, event types

### Phase 2: Core Processing
- Kafka consumer with graceful shutdown
- JSON Schema validation with S3 loader + auto-refresh
- SNS publisher with tenant_id message attribute
- DLQ producer with error envelope

### Phase 3: Mock Services
- Mock producer (multi-tenant + invalid events)
- Mock sender (SQS consumer + log)

### Phase 4: Infrastructure
- docker-compose.yml (Kafka + LocalStack)
- LocalStack init script (SNS, SQS, S3)
- Multi-stage Dockerfile

### Phase 5: Testing
- Unit tests for all internal packages
- Integration tests: godog features, testcontainers, step definitions

### Phase 6: Quality
- .golangci.yml tuning
- Coverage enforcement
- CI target validation
