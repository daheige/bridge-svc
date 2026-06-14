package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/daheige/bridge-svc/internal/config"
)

var tracer trace.Tracer

// InitTracer 初始化 OpenTelemetry Trace
// 将 Trace 数据通过 OTLP 发送到 OpenObserve
func InitTracer(cfg config.ObservabilityConfig) error {
	if cfg.TraceEndpoint == "" {
		tracer = otel.Tracer("bridge-svc")
		return nil
	}

	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(cfg.TraceEndpoint),
		otlptracegrpc.WithHeaders(map[string]string{
			"Authorization": "Bearer " + "cm9vdEBleGFtcGxlLmNvbTpLSHhLR2dNME9Dd2hSS3FY",
			"organization":  "default",
			"stream-name":   "default",
		}),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("create trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
		)),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer = provider.Tracer("bridge-svc")

	return nil
}

// ExtractTraceContext 从 gRPC metadata 提取 trace context
// 从业务方请求中提取 trace 信息，实现链路追踪透传
func ExtractTraceContext(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	carrier := propagation.MapCarrier{}
	for k, v := range md {
		if len(v) > 0 {
			carrier[k] = v[0]
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// StartSpan 启动新 span
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}
