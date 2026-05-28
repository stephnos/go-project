package observability

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Stack struct {
	Logger        *slog.Logger
	Tracer        trace.Tracer
	MeterProvider *sdkmetric.MeterProvider
	TraceProvider *sdktrace.TracerProvider
}

func New(ctx context.Context, serviceName string) (*Stack, func(context.Context) error, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, nil, err
	}
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter))

	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)))

	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	stack := &Stack{
		Logger:        logger.With("service", serviceName),
		Tracer:        otel.Tracer(serviceName),
		MeterProvider: meterProvider,
		TraceProvider: traceProvider,
	}
	shutdown := func(ctx context.Context) error {
		if err := meterProvider.Shutdown(ctx); err != nil {
			return err
		}
		return traceProvider.Shutdown(ctx)
	}
	return stack, shutdown, nil
}
