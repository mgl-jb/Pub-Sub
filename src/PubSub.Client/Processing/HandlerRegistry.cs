using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>
/// One registered handler, with the payload type it expects already bound in.
/// </summary>
/// <remarks>
/// The delegate is built once at registration, while the generic type argument is still known
/// statically. That keeps the per-message path free of reflection, and free of the
/// <c>MakeGenericMethod</c> calls that would otherwise break under trimming and Native AOT.
/// </remarks>
internal sealed class HandlerRegistration
{
    private readonly Func<IServiceProvider, DispatchContext, CancellationToken, Task> _invoke;

    private HandlerRegistration(
        string subject,
        Type payloadType,
        Func<IServiceProvider, DispatchContext, CancellationToken, Task> invoke)
    {
        Subject = subject;
        PayloadType = payloadType;
        _invoke = invoke;
    }

    /// <summary>The subject this handler is registered for.</summary>
    public string Subject { get; }

    /// <summary>The payload type the handler expects.</summary>
    public Type PayloadType { get; }

    /// <summary>Creates a registration bound to a payload type and handler implementation.</summary>
    public static HandlerRegistration Create<TMessage, THandler>(string subject)
        where THandler : class, IMessageHandler<TMessage>
    {
        return new HandlerRegistration(subject, typeof(TMessage), InvokeAsync);

        static async Task InvokeAsync(
            IServiceProvider services,
            DispatchContext dispatch,
            CancellationToken cancellationToken)
        {
            TMessage payload;

            try
            {
                payload = JsonSerializer.Deserialize<TMessage>(
                              dispatch.Envelope.Body.Span,
                              BrokerHttpClient.Json)
                          ?? throw new JsonException("The message body deserialized to null.");
            }
            catch (JsonException ex)
            {
                // A payload the handler cannot parse will not parse on the next attempt either, so
                // retrying it only burns the delivery budget. Dead-lettering surfaces it now, with
                // the parse error attached.
                await dispatch.Broker.DeadLetterAsync(
                    dispatch.Topic,
                    dispatch.Subscription,
                    dispatch.DeliveryId,
                    dispatch.LockToken,
                    DeadLetterReason.DeserializationError,
                    ex.Message,
                    cancellationToken);

                return;
            }

            BrokerMessageContext<TMessage> context = new(
                payload,
                dispatch.Envelope,
                dispatch.Broker,
                dispatch.Topic,
                dispatch.Subscription,
                dispatch.DeliveryId);

            IMessageHandler<TMessage> handler =
                ActivatorUtilities.GetServiceOrCreateInstance<THandler>(services);

            await handler.HandleAsync(context, cancellationToken);

            // A handler that settled explicitly has already claimed the right to do so, so
            // auto-complete does not fire a second time and fail with a lost lock.
            if (dispatch.AutoComplete && !context.IsSettled)
            {
                await context.CompleteAsync(cancellationToken);
            }
        }
    }

    /// <summary>Runs the handler for one message.</summary>
    public Task InvokeAsync(
        IServiceProvider services,
        DispatchContext dispatch,
        CancellationToken cancellationToken) =>
        _invoke(services, dispatch, cancellationToken);
}

/// <summary>Everything the dispatch delegate needs about one message.</summary>
internal sealed record DispatchContext(
    MessageEnvelope Envelope,
    BrokerHttpClient Broker,
    string Topic,
    string Subscription,
    long DeliveryId,
    Guid LockToken,
    bool AutoComplete);

/// <summary>
/// The handlers registered for a processor, indexed by subject.
/// </summary>
/// <remarks>
/// A single fallback handler may be registered to catch anything unmatched, which suits a
/// subscription whose filter already guarantees the shape of what arrives.
/// </remarks>
public sealed class HandlerRegistry
{
    private readonly Dictionary<string, HandlerRegistration> _bySubject = new(StringComparer.Ordinal);
    private HandlerRegistration? _fallback;

    /// <summary>Registers a handler for a subject.</summary>
    /// <param name="subject">The subject to route on; defaults to the payload type's name.</param>
    public HandlerRegistry Add<TMessage, THandler>(string? subject = null)
        where THandler : class, IMessageHandler<TMessage>
    {
        string resolved = subject ?? typeof(TMessage).Name;
        _bySubject[resolved] = HandlerRegistration.Create<TMessage, THandler>(resolved);
        return this;
    }

    /// <summary>Registers a handler for every message whose subject matches nothing else.</summary>
    public HandlerRegistry AddFallback<TMessage, THandler>()
        where THandler : class, IMessageHandler<TMessage>
    {
        _fallback = HandlerRegistration.Create<TMessage, THandler>("*");
        return this;
    }

    /// <summary>The subjects with a registered handler.</summary>
    public IReadOnlyCollection<string> Subjects => _bySubject.Keys;

    /// <summary>Finds the handler for a subject, falling back where one is registered.</summary>
    internal HandlerRegistration? Resolve(string? subject)
    {
        if (subject is not null && _bySubject.TryGetValue(subject, out HandlerRegistration? registration))
        {
            return registration;
        }

        return _fallback;
    }
}
