//go:build sender

package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/logger"
)

func main() {
	log := logger.Get("dummy-sender")

	cfg, err := config.LoadSenderConfig()
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

	sqsClient, err := aws.NewSQSClient(ctx, cfg.AWS)
	if err != nil {
		log.Error("failed to create SQS client", slog.Any("error", err))
		panic(err)
	}

	log.Info("sender started",
		slog.String("tenant_id", cfg.TenantID),
		slog.String("queue_url", cfg.QueueURL),
	)

	for {
		select {
		case <-ctx.Done():
			log.Info("sender stopping", slog.String("tenant_id", cfg.TenantID))
			return
		default:
			poll(ctx, log, sqsClient, cfg)
		}
	}
}

func poll(
	ctx context.Context,
	log *slog.Logger,
	client aws.SQSClient,
	cfg *config.SenderConfig,
) {
	messages, err := client.ReceiveMessages(ctx, cfg.QueueURL, cfg.MaxMessages)
	if err != nil {
		log.Error("failed to receive messages", slog.Any("error", err))
		return
	}

	for _, msg := range messages {
		evt := parseMessage(msg.Body)
		log.Info("received event",
			slog.String("tenant_id", cfg.TenantID),
			slog.String("event_id", evt.EventID),
			slog.String("event_type", evt.EventType),
			slog.Any("payload", evt.Payload),
		)

		if err = client.DeleteMessage(ctx, cfg.QueueURL, msg.ReceiptHandle); err != nil {
			log.Error("failed to delete message", slog.Any("error", err))
		}
	}
}

func parseMessage(body string) event.Event {
	var notification struct {
		Message string `json:"Message"`
	}

	if err := json.Unmarshal([]byte(body), &notification); err != nil {
		return event.Event{}
	}

	var evt event.Event
	if err := json.Unmarshal([]byte(notification.Message), &evt); err != nil {
		return event.Event{}
	}
	return evt
}
