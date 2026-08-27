using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>Publishes messages to the broker over HTTP.</summary>
internal sealed class EventPublisher : IEventPublisher
{
    private readonly BrokerHttpClient _broker;
    private readonly MessageTypeRegistry _types;
    private readonly PubSubClientOptions _options;

    public EventPublisher(
        BrokerHttpClient broker,
        MessageTypeRegistry types,
        IOptions<PubSubClientOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _broker = broker;
        _types = types;
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task<long> PublishAsync<T>(
        string topic,
        T payload,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(payload);

        PublishOptions publishOptions = new();
        configure?.Invoke(publishOptions);

        MessageEnvelope envelope = new()
        {
            MessageId = publishOptions.MessageId ?? Guid.NewGuid().ToString("n"),
            CorrelationId = publishOptions.CorrelationId,
            Subject = publishOptions.Subject ?? _types.SubjectFor(typeof(T)),
            ContentType = "application/json",
            Body = JsonSerializer.SerializeToUtf8Bytes(payload, BrokerHttpClient.Json),
            ApplicationProperties = new Dictionary<string, object?>(
                publishOptions.ApplicationProperties,
                StringComparer.Ordinal),
            SessionId = publishOptions.SessionId,
            ReplyTo = publishOptions.ReplyTo,
            ReplyToSessionId = publishOptions.ReplyToSessionId,
            To = publishOptions.To,
            ScheduledEnqueueTime = publishOptions.ScheduledEnqueueTime,
            TimeToLive = publishOptions.TimeToLive,
        };

        return await PublishAsync(topic, envelope, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<long> PublishAsync(
        string topic,
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<long> results = await PublishBatchAsync(topic, [message], cancellationToken);
        return results[0];
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> PublishBatchAsync(
        string topic,
        IEnumerable<MessageEnvelope> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        MessageEnvelope[] batch = [.. messages];
        if (batch.Length == 0)
        {
            return [];
        }

        using Activity? activity = PubSubDiagnostics.ActivitySource.StartActivity(
            $"publish {topic}",
            ActivityKind.Producer);

        activity?.SetTag("messaging.system", PubSubDiagnostics.SystemName);
        activity?.SetTag("messaging.operation.name", "publish");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.batch.message_count", batch.Length);

        WireMessage[] wire = new WireMessage[batch.Length];

        for (int i = 0; i < batch.Length; i++)
        {
            MessageEnvelope envelope = batch[i];

            // Trace context rides in the application properties so a consumer in another process
            // can link its work to the publish that caused it. Without this the two halves of a
            // message's journey appear as unrelated traces.
            Dictionary<string, object?> properties =
                new(envelope.ApplicationProperties, StringComparer.Ordinal);

            PubSubDiagnostics.InjectTraceContext(properties, activity);

            wire[i] = new WireMessage
            {
                MessageId = envelope.MessageId,
                CorrelationId = envelope.CorrelationId,
                Subject = envelope.Subject,
                ContentType = envelope.ContentType,
                Body = Convert.ToBase64String(envelope.Body.Span),
                ApplicationProperties = properties,
                SessionId = envelope.SessionId,
                ReplyTo = envelope.ReplyTo,
                ReplyToSessionId = envelope.ReplyToSessionId,
                To = envelope.To,
                ScheduledEnqueueTime = envelope.ScheduledEnqueueTime,
                TimeToLive = envelope.TimeToLive,
            };
        }

        IReadOnlyList<WirePublishResult> results =
            await _broker.PublishAsync(topic, wire, cancellationToken);

        PubSubDiagnostics.MessagesPublished.Add(
            results.Count,
            new KeyValuePair<string, object?>("messaging.destination.name", topic));

        return [.. results.Select(r => r.SequenceNumber)];
    }

    /// <inheritdoc />
    public Task<long> ScheduleAsync(
        string topic,
        MessageEnvelope message,
        DateTimeOffset enqueueAt,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        MessageEnvelope scheduled = new()
        {
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId,
            Subject = message.Subject,
            ContentType = message.ContentType,
            Body = message.Body,
            ApplicationProperties = message.ApplicationProperties,
            SessionId = message.SessionId,
            ReplyTo = message.ReplyTo,
            ReplyToSessionId = message.ReplyToSessionId,
            To = message.To,
            ScheduledEnqueueTime = enqueueAt,
            TimeToLive = message.TimeToLive,
        };

        return PublishAsync(topic, scheduled, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> CancelScheduledAsync(
        string topic,
        long sequenceNumber,
        CancellationToken cancellationToken = default) =>
        _broker.CancelScheduledAsync(topic, sequenceNumber, cancellationToken);
}
