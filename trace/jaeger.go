package trace

import (
	"github.com/chenjiahao-stl/framework/conf"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func InitJaegerTracer(config *conf.Data_Tracer, serviceName string) error {
	// Create the Jaeger exporter
	var exp *jaeger.Exporter
	var err error
	if config.GetAgentHost() == "" || config.GetAgentPort() == "" {
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.GetUrl())))
		if err != nil {
			return err
		}
	} else {
		exp, err = jaeger.New(jaeger.WithAgentEndpoint(jaeger.WithAgentHost(config.GetAgentHost()), jaeger.WithAgentPort(config.GetAgentPort())))
		if err != nil {
			return err
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
	return nil
}
