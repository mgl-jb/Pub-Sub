using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>A message delivered to a handler, settling through the broker's HTTP API.</summary>
internal sealed class BrokerMessageContext<T> : MessageContext<T>
{
    private readonly BrokerHttpClient _broker;
    private readonly string _topic;
    private readonly string _subscription;
    private readonly long _deliveryId;
    private int _settled;

    public BrokerMessageContext(
        T payload,
        MessageEnvelope envelope,
        BrokerHttpClient broker,
        string topic,
        string subscription,
        long deliveryId)
        : base(payload, envelope)
    {
        _broker = broker;
        _topic = topic;
        _subscription = subscription;
        _deliveryId = deliveryId;
    }

    /// <inheritdoc />
    public override bool IsSettled => Volatile.Read(ref _settled) == 1;

    /// <summary>The lock token this context settles with.</summary>
    public Guid LockToken => Envelope.LockToken
        ?? throw new InvalidOperationException("The message carries no lock token.");

    /// <inheritdoc />
    public override async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
        {
            return;
        }

        await _broker.CompleteAsync(_topic, _subscription, _deliveryId, LockToken, cancellationToken);
        Count("complete");
    }

    /// <inheritdoc />
    public override async Task AbandonAsync(
        IDictionary<string, object?>? propertiesToModify = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
        {
            return;
        }

        await _broker.AbandonAsync(
            _topic, _subscription, _deliveryId, LockToken, propertiesToModify, null, cancellationToken);

        Count("abandon");
    }

    /// <inheritdoc />
    public override async Task DeadLetterAsync(
        string reason,
        string? description = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
        {
            return;
        }

        await _broker.DeadLetterAsync(
            _topic, _subscription, _deliveryId, LockToken, reason, description, cancellationToken);

        Count("dead-letter");
    }

    /// <inheritdoc />
    public override async Task DeferAsync(
        IDictionary<string, object?>? propertiesToModify = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
        {
            return;
        }

        await _broker.DeferAsync(_topic, _subscription, _deliveryId, LockToken, cancellationToken);
        Count("defer");
    }

    /// <inheritdoc />
    public override Task<DateTimeOffset> RenewLockAsync(CancellationToken cancellationToken = default) =>
        _broker.RenewLockAsync(_topic, _subscription, _deliveryId, LockToken, cancellationToken);

    /// <summary>
    /// Claims the right to settle, exactly once.
    /// </summary>
    /// <remarks>
    /// A handler that settles explicitly and then returns would otherwise be settled a second time
    /// by the processor's auto-complete, and the second attempt fails with a lost lock — a
    /// confusing error for correct handler code. The interlock makes the first settlement win.
    /// </remarks>
    public bool TryMarkSettled() => Interlocked.Exchange(ref _settled, 1) == 0;

    private void Count(string outcome) =>
        PubSubDiagnostics.MessagesSettled.Add(
            1,
            new KeyValuePair<string, object?>("messaging.destination.name", _topic),
            new KeyValuePair<string, object?>("messaging.pubsub.subscription", _subscription),
            new KeyValuePair<string, object?>("messaging.pubsub.settlement", outcome));
}
