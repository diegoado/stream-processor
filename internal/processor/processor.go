package processor

import (
	"context"
	"log/slog"

	"github.com/diegoado/stream-processor/internal/consumer"
	"github.com/diegoado/stream-processor/internal/dlq"
	"github.com/diegoado/stream-processor/internal/handler"
	"github.com/diegoado/stream-processor/internal/publisher"
	"github.com/diegoado/stream-processor/internal/schema"
	"github.com/diegoado/stream-processor/pkg/aws"
	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/event"
	"github.com/diegoado/stream-processor/pkg/kafka"
	"github.com/diegoado/stream-processor/pkg/logger"
	"github.com/diegoado/stream-processor/pkg/telemetry"
)

// Processor wires and runs the stream processing pipeline.
type Processor interface {
	Start(ctx context.Context) error
}

type processorImpl struct {
	log *slog.Logger
	cfg *config.Config
}

// NewProcessor creates a Processor from the given configuration.
func NewProcessor(cfg *config.Config) Processor {
	return &processorImpl{log: logger.Get("stream-processor"), cfg: cfg}
}

// Start initializes all components and runs the consumer loop until the context is cancelled.
func (p *processorImpl) Start(ctx context.Context) error {
	otelShutdown := p.initTelemetry(ctx)
	schemaLoadCancelFunc, schemaValidator := p.initSchemaLoaderAndValidator(ctx)
	snsPublisher := p.initSnsPublisher(ctx)
	dlqProducer := p.initDlqProducer()
	kafkaConsumer := p.initKafkaConsumer()
	eventHandler := handler.NewHandler(schemaValidator, snsPublisher, dlqProducer)

	eventConsumer := consumer.NewConsumer(
		kafkaConsumer,
		eventHandler,
		p.cfg.Processor.EventsTopic,
		p.cfg.Processor.PollTimeout,
	)

	defer func() {
		p.log.Info("shutting down")

		if schemaLoadCancelFunc != nil {
			schemaLoadCancelFunc()
		}
		if dlqProducer != nil {
			dlqProducer.Close()
		}
		if otelShutdown != nil {
			_ = otelShutdown(ctx)
		}

		p.log.Info("shutdown complete")
	}()

	p.log.Info("stream processor starting")
	return eventConsumer.Start(ctx)
}

func (p *processorImpl) initSchemaLoaderAndValidator(
	ctx context.Context,
) (context.CancelFunc, schema.Validator) {
	s3Client, err := aws.NewS3Client(ctx, p.cfg.AWS)
	if err != nil {
		p.log.Error("failed to create S3 client", slog.Any("error", err))
		panic(err)
	}

	loader := schema.NewLoader(s3Client, p.cfg.Schema)
	data, etag, err := loader.Load(ctx)
	if err != nil {
		p.log.Error("failed to load schema", slog.Any("error", err))
		panic(err)
	}

	validator, err := schema.NewValidator(data, etag)
	if err != nil {
		p.log.Error("failed to compile schema", slog.Any("error", err))
		panic(err)
	}

	refreshCtx, schemaLoadCancel := context.WithCancel(ctx)
	loader.StartAutoRefresh(refreshCtx, validator)

	return schemaLoadCancel, validator
}

func (p *processorImpl) initSnsPublisher(ctx context.Context) publisher.Publisher {
	snsClient, err := aws.NewSNSClient(ctx, p.cfg.AWS)
	if err != nil {
		p.log.Error("failed to create SNS client", slog.Any("error", err))
		panic(err)
	}
	return publisher.NewPublisher(snsClient, p.cfg.Processor.AcceptedEventsTopic)
}

func (p *processorImpl) initDlqProducer() dlq.Producer {
	kafkaProducer, err := kafka.NewSyncProducer[event.RejectedEvent](p.cfg.Kafka)
	if err != nil {
		p.log.Error("failed to create DLQ producer", slog.Any("error", err))
		panic(err)
	}
	return dlq.NewProducer(kafkaProducer, p.cfg.Processor.RejectedEventsTopic)
}

func (p *processorImpl) initKafkaConsumer() kafka.Consumer[event.Event] {
	kafkaConsumer, err := kafka.NewConsumer[event.Event](p.cfg.Kafka)
	if err != nil {
		p.log.Error("failed to create Kafka consumer", slog.Any("error", err))
		panic(err)
	}
	return kafkaConsumer
}

func (p *processorImpl) initTelemetry(ctx context.Context) func(context.Context) error {
	if !p.cfg.OTel.Enabled {
		return nil
	}

	provider, err := telemetry.Setup(ctx, p.cfg.OTel)
	if err != nil {
		p.log.Error("failed to setup telemetry", slog.Any("error", err))
		return nil
	}

	p.log.Info("telemetry initialized")
	return provider.Shutdown
}
