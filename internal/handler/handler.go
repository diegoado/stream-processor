package handler

import (
	"context"
	"log/slog"

	"github.com/diegoado/stream-processor/internal/dlq"
	"github.com/diegoado/stream-processor/internal/publisher"
	"github.com/diegoado/stream-processor/internal/schema"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Handler orchestrates event validation, sanitization, and routing.
type Handler interface {
	Handle(ctx context.Context, evt event.Event) error
}

type handlerImpl struct {
	log       *slog.Logger
	validator schema.Validator
	publisher publisher.Publisher
	rejecter  dlq.Producer
}

// NewHandler creates a Handler with the given dependencies.
func NewHandler(validator schema.Validator, publisher publisher.Publisher, rejecter dlq.Producer) Handler {
	return &handlerImpl{log: logger.Get("handler"), validator: validator, publisher: publisher, rejecter: rejecter}
}

// Handle validates an event, sanitizes its payload if valid and publishes to SNS, or rejects to DLQ.
func (h *handlerImpl) Handle(ctx context.Context, evt event.Event) error {
	sanitized, errors, err := h.validator.ValidateAndSanitize(evt)
	if err != nil {
		return err
	}

	if len(errors) > 0 {
		h.log.Warn("event rejected", slog.String("event_id", evt.EventID), slog.Any("errors", errors))
		return h.rejecter.Send(ctx, evt, errors)
	}

	h.log.Info("event accepted", slog.String("event_id", evt.EventID), slog.String("tenant_id", evt.TenantID))
	return h.publisher.Publish(ctx, sanitized)
}
