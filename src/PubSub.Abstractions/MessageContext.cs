namespace PubSub.Abstractions;

/// <summary>
/// A message delivered to a handler, together with the operations that settle it.
/// </summary>
/// <typeparam name="T">The deserialized payload type.</typeparam>
/// <remarks>
/// Settlement is explicit but rarely needed: the processor completes the message when the handler
/// returns and abandons it when the handler throws. Call these methods only to override that —
/// to dead-letter a message that can never succeed, or to defer one whose turn has not come.
/// </remarks>
public abstract class MessageContext<T>
{
    /// <summary>Creates a context around a payload and its envelope.</summary>
    protected MessageContext(T payload, MessageEnvelope envelope)
    {
        Payload = payload;
        Envelope = envelope;
    }

    /// <summary>The deserialized payload.</summary>
    public T Payload { get; }

    /// <summary>The full message, including routing metadata and delivery state.</summary>
    public MessageEnvelope Envelope { get; }

    /// <summary>Shorthand for <see cref="MessageEnvelope.MessageId"/>.</summary>
    public string MessageId => Envelope.MessageId;

    /// <summary>
    /// How many times this message has been delivered, including now. A value above 1 means an
    /// earlier attempt did not settle — treat the work as possibly already done.
    /// </summary>
    public int DeliveryCount => Envelope.DeliveryCount;

    /// <summary>The session this message belongs to, when the subscription is session-enabled.</summary>
    public string? SessionId => Envelope.SessionId;

    /// <summary>Whether the message has already been settled by this handler.</summary>
    public abstract bool IsSettled { get; }

    /// <summary>
    /// Settles the message successfully; it will not be delivered again.
    /// </summary>
    /// <exception cref="MessageLockLostException">The lock expired before settlement.</exception>
    public abstract Task CompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the message back to the subscription for redelivery, incrementing its delivery
    /// count. Use for transient failures.
    /// </summary>
    /// <param name="propertiesToModify">Properties to merge into the message before it is redelivered.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    public abstract Task AbandonAsync(
        IDictionary<string, object?>? propertiesToModify = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves the message to the dead-letter queue without further retries. Use when the message
    /// can never be processed successfully — bad data, a rejected schema — so that retrying only
    /// wastes attempts.
    /// </summary>
    /// <param name="reason">A short, filterable cause. See <see cref="DeadLetterReason"/>.</param>
    /// <param name="description">Free-text detail for whoever triages the queue.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    public abstract Task DeadLetterAsync(
        string reason,
        string? description = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the message aside without completing it. A deferred message is no longer delivered in
    /// the normal flow and can only be retrieved by its
    /// <see cref="MessageEnvelope.SequenceNumber"/>, so record that number somewhere durable
    /// first — otherwise the message is stranded until its time to live expires.
    /// </summary>
    public abstract Task DeferAsync(
        IDictionary<string, object?>? propertiesToModify = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Extends the peek-lock for work that is legitimately slow. Usually unnecessary — the
    /// processor renews locks automatically up to a configured ceiling.
    /// </summary>
    /// <returns>The new lock expiry.</returns>
    /// <exception cref="MessageLockLostException">The lock had already expired.</exception>
    public abstract Task<DateTimeOffset> RenewLockAsync(CancellationToken cancellationToken = default);
}
