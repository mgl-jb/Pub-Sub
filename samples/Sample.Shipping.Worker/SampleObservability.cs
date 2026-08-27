using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PubSub.Client;

namespace Sample.Shipping.Worker;

/// <summary>Tracing and metrics for the sample services.</summary>
public static class SampleObservability
{
    /// <summary>
    /// Registers OpenTelemetry with the PubSub client's activity source included.
    /// </summary>
    /// <remarks>
    /// Including the client's source is what makes the demonstration worth running: a publish here
    /// and the consume in the worker land in one trace, which is the question people actually ask
    /// of a messaging system — what happened to this message?
    /// </remarks>
    public static IServiceCollection AddSampleObservability(
        this IServiceCollection services,
        string serviceName)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing => tracing
                .AddSource(PubSubDiagnostics.ActivitySourceName)
                .AddHttpClientInstrumentation())
            .WithMetrics(metrics => metrics
                .AddMeter(PubSubDiagnostics.MeterName)
                .AddHttpClientInstrumentation());

        return services;
    }
}
