package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/diegoado/stream-processor/pkg/config"
	"github.com/diegoado/stream-processor/pkg/logger"
)

// Provider manages OpenTelemetry tracer, meter, and log providers.
type Provider interface {
	Shutdown(ctx context.Context) error
}

type providerImpl struct {
	tp *trace.TracerProvider
	mp *metric.MeterProvider
	lp *log.LoggerProvider
}

// Setup initializes OpenTelemetry providers and returns a Provider.
func Setup(ctx context.Context, cfg config.OTelConfig) (Provider, error) {
	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp, err := newTracerProvider(ctx, res, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracer provider: %w", err)
	}

	mp, err := newMeterProvider(ctx, res)
	if err != nil {
		return nil, fmt.Errorf("failed to create meter provider: %w", err)
	}

	lp, err := newLoggerProvider(ctx, res)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger provider: %w", err)
	}

	return &providerImpl{tp: tp, mp: mp, lp: lp}, nil
}

// Shutdown flushes and shuts down all providers.
func (p *providerImpl) Shutdown(ctx context.Context) error {
	return errors.Join(p.tp.Shutdown(ctx), p.mp.Shutdown(ctx), p.lp.Shutdown(ctx))
}

func newResource(ctx context.Context, cfg config.OTelConfig) (*resource.Resource, error) {
	hostName, _ := os.Hostname()

	return resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName), semconv.HostName(hostName)),
	)
}

func newTracerProvider(
	ctx context.Context,
	res *resource.Resource,
	cfg config.OTelConfig,
) (*trace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(trace.WithBatcher(exporter), trace.WithResource(res))
	otel.SetTracerProvider(tp)
	logger.SetOTelHandler(cfg.ServiceName)

	return tp, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource) (*metric.MeterProvider, error) {
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}

	mp := metric.NewMeterProvider(metric.WithReader(metric.NewPeriodicReader(exporter)), metric.WithResource(res))
	otel.SetMeterProvider(mp)

	return mp, nil
}

func newLoggerProvider(ctx context.Context, res *resource.Resource) (*log.LoggerProvider, error) {
	exporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, err
	}

	lp := log.NewLoggerProvider(log.WithProcessor(log.NewBatchProcessor(exporter)), log.WithResource(res))
	global.SetLoggerProvider(lp)

	return lp, nil
}
