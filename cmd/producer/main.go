package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
	"github.com/diegoado/stream-processor/pkg/logger"
)

type variant int

const (
	normalVariant     variant = 0
	overloadedVariant variant = 1
	distortedVariant  variant = 2
)

func main() {
	log := logger.Get("dummy-producer")

	cfg, err := config.LoadProducerConfig()
	if err != nil {
		log.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	producer, err := kafka.NewProducer[event.Event](cfg.Kafka)
	if err != nil {
		log.Error("failed to create producer", slog.Any("error", err))
		panic(err)
	}
	defer producer.Close()

	log.Info("producer started", slog.String("topic", cfg.EventsTopic))

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	count := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("producer stopping")
			producer.Flush()
			return
		case <-ticker.C:
			count++

			evt := createNewEvent(getTargetVariant(count, cfg))

			err = producer.Produce(ctx, kafka.ProducerMessage[event.Event]{
				Topic: cfg.EventsTopic,
				Key:   &kafka.MessageKey{TenantID: evt.TenantID, EventType: evt.EventType},
				Value: evt,
			})
			if err != nil {
				log.Error("failed to produce", slog.Any("error", err))
				continue
			}
			log.Info("produced event",
				slog.String("event_id", evt.EventID),
				slog.String("tenant_id", evt.TenantID),
				slog.String("event_type", evt.EventType),
				slog.Bool("is_invalid", count%cfg.InvalidEventFrequency == 0),
				slog.Bool("has_extra_fields", count%cfg.ExtraFieldFrequency == 0),
			)
		}
	}
}

func getTargetVariant(scenario int, cfg *config.ProducerConfig) variant {
	switch {
	case scenario%cfg.InvalidEventFrequency == 0:
		return distortedVariant
	case scenario%cfg.ExtraFieldFrequency == 0:
		return overloadedVariant
	default:
		return normalVariant
	}
}

func createNewEvent(variant variant) event.Event {
	tenantList := []string{"tenant-a", "tenant-b", "tenant-c"}
	eventTypes := []string{
		"monitoring.alert",
		"monitoring.metric",
		"user.action",
		"transaction.auth",
		"webhook.dispatched",
	}

	tenantUID := tenantList[rand.IntN(len(tenantList))]
	eventType := eventTypes[rand.IntN(len(eventTypes))]

	switch {
	case distortedVariant == variant:
		return createEventWithError(tenantUID, eventType)
	case overloadedVariant == variant:
		return createEventWithExtraFields(tenantUID, eventType)
	default:
		return createEvent(tenantUID, eventType)
	}
}

func createEvent(tenantID, eventType string) event.Event {
	return event.Event{
		EventID:   uuid.NewString(),
		EventType: eventType,
		TenantID:  tenantID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payloadFor(tenantID, eventType),
	}
}

func createEventWithExtraFields(tenantID, eventType string) event.Event {
	payload := payloadFor(tenantID, eventType)
	payload["unexpected_field"] = "should_be_stripped"

	return event.Event{
		EventID:   uuid.NewString(),
		EventType: eventType,
		TenantID:  tenantID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}
}

func createEventWithError(tenantID, eventType string) event.Event {
	variants := 3
	switch rand.IntN(variants) {
	case 0:
		return event.Event{
			EventType: eventType,
			TenantID:  tenantID,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   payloadFor(tenantID, eventType),
		}
	case 1:
		return event.Event{
			EventID:   uuid.NewString(),
			EventType: eventType,
			TenantID:  tenantID,
			Timestamp: "not-a-date",
			Payload:   payloadFor(tenantID, eventType),
		}
	default:
		return event.Event{
			EventID:   uuid.NewString(),
			EventType: eventType,
			TenantID:  tenantID,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   map[string]any{},
		}
	}
}

func payloadFor(tenantID, eventType string) map[string]any {
	base := defaultPayload(eventType)
	changedPayload(base, tenantID, eventType)
	return base
}

func defaultPayload(eventType string) map[string]any {
	switch eventType {
	case "monitoring.alert":
		return map[string]any{
			"severity": "high",
			"source":   "cpu-monitor",
			"message":  "CPU usage above 90%",
		}
	case "monitoring.metric":
		return map[string]any{
			"metric_name": "cpu_usage",
			"value":       85.5,
			"unit":        "percent",
		}
	case "user.action":
		return map[string]any{
			"action":   "click",
			"user_id":  uuid.NewString(),
			"resource": "/dashboard",
		}
	case "transaction.auth":
		return map[string]any{
			"transaction_id": uuid.NewString(),
			"amount":         99.99,
			"currency":       "BRL",
			"status":         "approved",
		}
	case "webhook.dispatched":
		return map[string]any{
			"webhook_id":  uuid.NewString(),
			"endpoint":    "https://example.com/webhook",
			"http_status": 200,
		}
	default:
		return map[string]any{}
	}
}

func changedPayload(payload map[string]any, tenantID, eventType string) {
	switch {
	case tenantID == "tenant-a" && eventType == "monitoring.alert":
		payload["alert_url"] = "https://alerts.example.com/12345"
	case tenantID == "tenant-a" && eventType == "transaction.auth":
		payload["risk_score"] = 0.42
	case tenantID == "tenant-b" && eventType == "user.action":
		payload["session_id"] = uuid.NewString()
	case tenantID == "tenant-c" && eventType == "webhook.dispatched":
		payload["callback_id"] = uuid.NewString()
	}
}
