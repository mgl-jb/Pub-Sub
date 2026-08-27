using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>
/// Resolves the handler for a message and runs it, settling according to the outcome.
/// </summary>
/// <remarks>
/// Each message is handled in its own dependency-injection scope, so a handler can depend on
/// scoped services — a database context, a unit of work — exactly as an HTTP request handler
/// would, and those are disposed when the message is done rather than living for the lifetime of
/// the processor.
/// </remarks>
internal sealed class MessageDispatcher
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly HandlerRegistry _handlers;
    private readonly BrokerHttpClient _broker;
    private readonly ILogger<MessageDispatcher> _logger;

    public MessageDispatcher(
        IServiceScopeFactory scopeFactory,
        HandlerRegistry handlers,
        BrokerHttpClient broker,
        ILogger<MessageDispatcher> logger)
    {
        _scopeFactory = scopeFactory;
        _handlers = handlers;
        _broker = broker;
        _logger = logger;
    }

    /// <summary>Handles one message end to end, including settlement.</summary>
    public async Task DispatchAsync(
        string topic,
        string subscription,
        WireReceivedMessage received,
        bool autoComplete,
        CancellationToken cancellationToken)
    {
        MessageEnvelope envelope = ToEnvelope(received);

        // Linking to the producer's context rather than starting a fresh trace is what makes the
        // publish and the consume one story.
        ActivityContext? parent = PubSubDiagnostics.ExtractTraceContext(
            (IReadOnlyDictionary<string, object?>)envelope.ApplicationProperties);

        using Activity? activity = PubSubDiagnostics.ActivitySource.StartActivity(
            $"process {topic}/{subscription}",
            ActivityKind.Consumer,
            parent ?? default);

        activity?.SetTag("messaging.system", PubSubDiagnostics.SystemName);
        activity?.SetTag("messaging.operation.name", "process");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.pubsub.subscription", subscription);
        activity?.SetTag("messaging.message.id", envelope.MessageId);
        activity?.SetTag("messaging.pubsub.delivery_count", envelope.DeliveryCount);

        PubSubDiagnostics.MessagesReceived.Add(
            1,
            new KeyValuePair<string, object?>("messaging.destination.name", topic),
            new KeyValuePair<string, object?>("messaging.pubsub.subscription", subscription));

        HandlerRegistration? registration = _handlers.Resolve(envelope.Subject);

        if (registration is null)
        {
            // A message nobody handles is a routing or configuration error, and retrying it will
            // never help — the handler will still be missing on the next attempt.
            ClientLog.NoHandlerRegistered(
                _logger, envelope.Subject, topic, subscription, envelope.MessageId);

            await _broker.DeadLetterAsync(
                topic,
                subscription,
                received.DeliveryId,
                received.LockToken,
                DeadLetterReason.ApplicationError,
                $"No handler is registered for subject '{envelope.Subject}'.",
                cancellationToken);

            return;
        }

        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        long startedAt = Stopwatch.GetTimestamp();

        try
        {
            await registration.InvokeAsync(
                scope.ServiceProvider,
                new DispatchContext(
                    envelope,
                    _broker,
                    topic,
                    subscription,
                    received.DeliveryId,
                    received.LockToken,
                    autoComplete),
                cancellationToken);

            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);

            PubSubDiagnostics.HandlerFailures.Add(
                1,
                new KeyValuePair<string, object?>("messaging.destination.name", topic),
                new KeyValuePair<string, object?>("messaging.pubsub.subscription", subscription),
                new KeyValuePair<string, object?>("error.type", ex.GetType().Name));

            ClientLog.HandlerFailed(
                _logger, ex, envelope.MessageId, topic, subscription, envelope.DeliveryCount);

            if (autoComplete)
            {
                await AbandonQuietlyAsync(topic, subscription, received, cancellationToken);
            }
        }
        finally
        {
            PubSubDiagnostics.HandlerDuration.Record(
                Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
                new KeyValuePair<string, object?>("messaging.destination.name", topic),
                new KeyValuePair<string, object?>("messaging.pubsub.subscription", subscription));
        }
    }

    /// <summary>
    /// Abandons after a handler failure, tolerating a lock that has already lapsed.
    /// </summary>
    /// <remarks>
    /// If the lock expired while the handler ran, the message is already back on the subscription.
    /// Surfacing that as a second error would obscure the original failure, which is the one worth
    /// reading.
    /// </remarks>
    private async Task AbandonQuietlyAsync(
        string topic,
        string subscription,
        WireReceivedMessage received,
        CancellationToken cancellationToken)
    {
        try
        {
            await _broker.AbandonAsync(
                topic, subscription, received.DeliveryId, received.LockToken, null, null, cancellationToken);
        }
        catch (MessageLockLostException)
        {
            ClientLog.LockAlreadyLapsed(_logger, received.DeliveryId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ClientLog.AbandonFailed(_logger, ex, received.DeliveryId);
        }
    }

    private static MessageEnvelope ToEnvelope(WireReceivedMessage received)
    {
        WireMessage message = received.Message;

        return new MessageEnvelope
        {
            MessageId = message.MessageId ?? string.Empty,
            CorrelationId = message.CorrelationId,
            Subject = message.Subject,
            ContentType = message.ContentType,
            Body = string.IsNullOrEmpty(message.Body) ? default : Convert.FromBase64String(message.Body),
            ApplicationProperties = message.ApplicationProperties is null
                ? new Dictionary<string, object?>(StringComparer.Ordinal)
                : new Dictionary<string, object?>(message.ApplicationProperties, StringComparer.Ordinal),
            SessionId = message.SessionId,
            ReplyTo = message.ReplyTo,
            ReplyToSessionId = message.ReplyToSessionId,
            To = message.To,
            SequenceNumber = message.SequenceNumber,
            EnqueuedTime = message.EnqueuedTime,
            DeliveryCount = message.DeliveryCount,
            LockToken = received.LockToken,
            LockedUntil = received.LockedUntil,
            State = MessageState.Locked,
            DeadLetterReason = message.DeadLetterReason,
            DeadLetterDescription = message.DeadLetterDescription,
        };
    }
}
