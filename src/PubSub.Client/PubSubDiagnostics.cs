using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PubSub.Client;

/// <summary>
/// Tracing and metrics for the client, following OpenTelemetry's messaging conventions.
/// </summary>
/// <remarks>
/// The point of instrumenting a messaging client is that a request's story is split across
/// processes: without propagated trace context a publish and the consume it caused look like two
/// unrelated traces, and the question people actually ask — "what happened to this message?" —
/// becomes unanswerable.
/// </remarks>
public static class PubSubDiagnostics
{
    /// <summary>The value reported as <c>messaging.system</c>.</summary>
    public const string SystemName = "pubsub";

    /// <summary>The name of the activity source, for OpenTelemetry registration.</summary>
    public const string ActivitySourceName = "PubSub.Client";

    /// <summary>The name of the meter, for OpenTelemetry registration.</summary>
    public const string MeterName = "PubSub.Client";

    /// <summary>The application property carrying the W3C traceparent.</summary>
    public const string TraceParentProperty = "traceparent";

    /// <summary>The application property carrying the W3C tracestate.</summary>
    public const string TraceStateProperty = "tracestate";

    /// <summary>Activities the client emits.</summary>
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    /// <summary>Messages accepted by the broker.</summary>
    public static readonly Counter<long> MessagesPublished =
        Meter.CreateCounter<long>("pubsub.client.published", "{message}", "Messages published.");

    /// <summary>Messages handed to a handler.</summary>
    public static readonly Counter<long> MessagesReceived =
        Meter.CreateCounter<long>("pubsub.client.received", "{message}", "Messages received.");

    /// <summary>Messages settled, tagged by how.</summary>
    public static readonly Counter<long> MessagesSettled =
        Meter.CreateCounter<long>("pubsub.client.settled", "{message}", "Messages settled.");

    /// <summary>Handler failures.</summary>
    public static readonly Counter<long> HandlerFailures =
        Meter.CreateCounter<long>("pubsub.client.handler_failures", "{failure}", "Handler failures.");

    /// <summary>How long handlers take.</summary>
    public static readonly Histogram<double> HandlerDuration =
        Meter.CreateHistogram<double>(
            "pubsub.client.handler.duration", "ms", "Time spent in a message handler.");

    /// <summary>Writes the current trace context into a message's properties.</summary>
    public static void InjectTraceContext(IDictionary<string, object?> properties, Activity? activity)
    {
        ArgumentNullException.ThrowIfNull(properties);

        Activity? source = activity ?? Activity.Current;
        if (source is null)
        {
            return;
        }

        properties[TraceParentProperty] = source.Id;

        if (!string.IsNullOrEmpty(source.TraceStateString))
        {
            properties[TraceStateProperty] = source.TraceStateString;
        }
    }

    /// <summary>Reads the trace context a producer wrote into a message's properties.</summary>
    /// <returns>The parent context, or <c>null</c> when the message carries none.</returns>
    public static ActivityContext? ExtractTraceContext(IReadOnlyDictionary<string, object?> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);

        if (!properties.TryGetValue(TraceParentProperty, out object? parent)
            || parent is not string traceParent
            || string.IsNullOrEmpty(traceParent))
        {
            return null;
        }

        string? traceState = properties.TryGetValue(TraceStateProperty, out object? state)
            ? state as string
            : null;

        return ActivityContext.TryParse(traceParent, traceState, out ActivityContext context)
            ? context
            : null;
    }
}
