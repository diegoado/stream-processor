package handler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/diegoado/stream-processor/internal/handler"
	"github.com/diegoado/stream-processor/pkg/event"
)

func TestHandler_Handle(t *testing.T) {
	var validator *ValidatorMock
	var publisher *PublisherMock
	var rejecter *RejecterMock
	var suite handler.Handler

	setup := func() {
		validator = new(ValidatorMock)
		publisher = new(PublisherMock)
		rejecter = new(RejecterMock)
		suite = handler.NewHandler(validator, publisher, rejecter)
	}

	validEvent := event.Event{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		EventType: "monitoring.alert",
		TenantID:  "tenant-a",
		Timestamp: "2026-05-01T00:00:00Z",
		Payload:   map[string]any{"severity": "high", "message": "alert"},
	}

	sanitizedEvent := &event.Event{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		EventType: "monitoring.alert",
		TenantID:  "tenant-a",
		Timestamp: "2026-05-01T00:00:00Z",
		Payload:   map[string]any{"severity": "high", "message": "alert"},
	}

	t.Run("should publish sanitized event when validation passes", func(t *testing.T) {
		setup()
		validator.On("ValidateAndSanitize", validEvent).
			Return(sanitizedEvent, []string(nil), nil)
		publisher.On("Publish", mock.Anything, sanitizedEvent).
			Return(nil)

		err := suite.Handle(context.Background(), validEvent)

		require.NoError(t, err)
		validator.AssertExpectations(t)
		publisher.AssertExpectations(t)
		rejecter.AssertNotCalled(t, "Send")
	})

	t.Run("should reject event to DLQ when validation fails", func(t *testing.T) {
		setup()
		validationErrors := []string{"event_id is required"}
		validator.On("ValidateAndSanitize", validEvent).
			Return((*event.Event)(nil), validationErrors, nil)
		rejecter.On("Send", mock.Anything, validEvent, validationErrors).
			Return(nil)

		err := suite.Handle(context.Background(), validEvent)

		require.NoError(t, err)
		validator.AssertExpectations(t)
		rejecter.AssertExpectations(t)
		publisher.AssertNotCalled(t, "Publish")
	})

	t.Run("should return error when validator fails", func(t *testing.T) {
		setup()
		validator.On("ValidateAndSanitize", validEvent).
			Return((*event.Event)(nil), []string(nil), assert.AnError)

		err := suite.Handle(context.Background(), validEvent)

		require.Error(t, err)
		publisher.AssertNotCalled(t, "Publish")
		rejecter.AssertNotCalled(t, "Send")
	})

	t.Run("should return error when publisher fails", func(t *testing.T) {
		setup()
		validator.On("ValidateAndSanitize", validEvent).
			Return(sanitizedEvent, []string(nil), nil)
		publisher.On("Publish", mock.Anything, sanitizedEvent).
			Return(assert.AnError)

		err := suite.Handle(context.Background(), validEvent)

		require.Error(t, err)
		validator.AssertExpectations(t)
		publisher.AssertExpectations(t)
	})

	t.Run("should return error when rejecter fails", func(t *testing.T) {
		setup()
		validationErrors := []string{"bad event"}
		validator.On("ValidateAndSanitize", validEvent).
			Return((*event.Event)(nil), validationErrors, nil)
		rejecter.On("Send", mock.Anything, validEvent, validationErrors).
			Return(assert.AnError)

		err := suite.Handle(context.Background(), validEvent)

		require.Error(t, err)
		validator.AssertExpectations(t)
		rejecter.AssertExpectations(t)
	})
}
