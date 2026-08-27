using Azure.Monitor.OpenTelemetry.AspNetCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace PubSub.Broker.Api;

/// <summary>Wires up tracing and metrics.</summary>
public static class Observability
{
    /// <summary>The activity source the broker emits spans on.</summary>
    public const string ActivitySourceName = "PubSub.Broker";

    /// <summary>
    /// Adds OpenTelemetry, exporting to Application Insights or an OTLP endpoint where configured.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A message's story spans processes: it is published by one service, brokered here, and
    /// handled by another. Traces are the only view that reassembles it, which is why the client
    /// propagates W3C context through message properties and why this registers the client's
    /// activity source alongside the broker's — a span from either side lands in the same trace.
    /// </para>
    /// <para>
    /// Both exporters are optional. With neither configured the instrumentation still runs and
    /// costs almost nothing, so a deployment that has not chosen a backend yet is not a special
    /// case in the code.
    /// </para>
    /// </remarks>
    public static IHostApplicationBuilder AddPubSubObservability(this IHostApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        string serviceName = builder.Configuration["Broker:ServiceName"] ?? "pubsub-broker";

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(
                serviceName,
                serviceVersion: typeof(Observability).Assembly.GetName().Version?.ToString(),
                serviceInstanceId: Environment.MachineName))
            .WithTracing(tracing => tracing
                .AddSource(ActivitySourceName)
                .AddSource(PubSub.Client.PubSubDiagnostics.ActivitySourceName)
                .AddAspNetCoreInstrumentation(options =>
                {
                    // Health probes fire constantly and say nothing about message flow; tracing
                    // them buries the spans that matter.
                    options.Filter = context =>
                        !context.Request.Path.StartsWithSegments("/health");
                })
                .AddHttpClientInstrumentation())
            .WithMetrics(metrics => metrics
                .AddMeter(PubSub.Client.PubSubDiagnostics.MeterName)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation());

        string? applicationInsights = builder.Configuration["ApplicationInsights:ConnectionString"]
                                      ?? builder.Configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];

        if (!string.IsNullOrWhiteSpace(applicationInsights))
        {
            builder.Services.AddOpenTelemetry().UseAzureMonitor(options =>
                options.ConnectionString = applicationInsights);
        }

        string? otlpEndpoint = builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"];

        if (!string.IsNullOrWhiteSpace(otlpEndpoint))
        {
            builder.Services.AddOpenTelemetry()
                .WithTracing(tracing => tracing.AddOtlpExporter())
                .WithMetrics(metrics => metrics.AddOtlpExporter());
        }

        return builder;
    }
}
