package trace

import (
	"context"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/go-kratos/kratos/v2/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"time"
)

func InitJaegerTracer(config *conf.Data_Tracer, serviceName string) (*trace.TracerProvider, func(), error) {
	// Create the Jaeger exporter
	var exp *jaeger.Exporter
	var err error
	if config.GetAgentHost() == "" || config.GetAgentPort() == "" {
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.GetUrl())))
		if err != nil {
			return nil, nil, err
		}
	} else {
		exp, err = jaeger.New(jaeger.WithAgentEndpoint(jaeger.WithAgentHost(config.GetAgentHost()), jaeger.WithAgentPort(config.GetAgentPort())))
		if err != nil {
			return nil, nil, err
		}
	}

	tp := trace.NewTracerProvider(
		// Set the sampling rate based on the parent span to 100%
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(1.0))),
		// Always be sure to batch in production.
		trace.WithBatcher(exp),
		// Record information about this application in an Resource.
		trace.WithResource(resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, func() {
		_ = tp.Shutdown(nil) // 关闭 Tracer
	}, nil
}

// ShutdownTracer 用于关闭 Jaeger 和 OpenTelemetry
func ShutdownTracer(tp *trace.TracerProvider) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tp.Shutdown(ctx); err != nil {
		log.Fatalf("failed to stop TracerProvider: %v", err)
	}
}
