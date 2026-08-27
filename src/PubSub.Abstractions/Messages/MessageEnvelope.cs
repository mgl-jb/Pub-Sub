using System.Collections.ObjectModel;

namespace PubSub.Abstractions;

/// <summary>
/// A message as it travels through the system: the payload plus the metadata the broker routes,
/// orders, deduplicates, and traces on.
/// </summary>
/// <remarks>
/// Properties fall into two groups. Producers set the first group. The broker assigns the second
/// on publish and updates it on each delivery attempt — a producer's values for those are ignored.
/// </remarks>
public sealed class MessageEnvelope
{
    /// <summary>
    /// Producer-assigned identity, used for duplicate detection on send and as the natural
    /// deduplication key for idempotent consumers. Defaults to a new GUID.
    /// </summary>
    /// <remarks>
    /// Set this to a business identifier (an order id, a request id) rather than letting it default
    /// whenever you want a retried send to be recognised as the same message.
    /// </remarks>
    public string MessageId { get; set; } = Guid.NewGuid().ToString("n");

    /// <summary>Ties a message to the conversation it belongs to; matched by correlation filters.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// The message's type or label — the primary routing discriminator, and what the client library
    /// maps to a CLR type when dispatching to a handler.
    /// </summary>
    public string? Subject { get; init; }

    /// <summary>MIME type of <see cref="Body"/>. Defaults to <c>application/json</c>.</summary>
    public string ContentType { get; init; } = "application/json";

    /// <summary>The payload. The broker treats this as opaque bytes and never parses it.</summary>
    public ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>
    /// Producer-defined properties. Subscription filters read these, so keep them small and
    /// scalar — routing metadata, not a second copy of the payload.
    /// </summary>
    public IDictionary<string, object?> ApplicationProperties { get; init; } =
        new Dictionary<string, object?>(StringComparer.Ordinal);

    /// <summary>
    /// Groups messages that must be processed in order by one consumer at a time. Messages sharing
    /// a session id are delivered strictly in sequence; messages without one are delivered
    /// concurrently to competing consumers.
    /// </summary>
    public string? SessionId { get; init; }

    /// <summary>Where a reply should be sent, for request/reply exchanges.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Session to reply on, for request/reply exchanges over sessions.</summary>
    public string? ReplyToSessionId { get; init; }

    /// <summary>Intended destination, carried for the application's own routing.</summary>
    public string? To { get; init; }

    /// <summary>
    /// Withholds the message until this instant. The message is stored immediately but stays
    /// invisible to receivers until the time passes.
    /// </summary>
    public DateTimeOffset? ScheduledEnqueueTime { get; init; }

    /// <summary>
    /// How long the message stays eligible for delivery. On expiry it is dead-lettered when the
    /// subscription enables that, and discarded otherwise. Falls back to the topic's default.
    /// </summary>
    public TimeSpan? TimeToLive { get; init; }

    // --- Broker-assigned. Values supplied by a producer are ignored on publish. ---

    /// <summary>
    /// Monotonically increasing per topic, assigned at publish. Defines delivery order within a
    /// session and identifies a deferred message for later retrieval.
    /// </summary>
    public long SequenceNumber { get; init; }

    /// <summary>When the broker accepted the message.</summary>
    public DateTimeOffset EnqueuedTime { get; init; }

    /// <summary>
    /// How many times this delivery has been handed to a receiver, including the current attempt.
    /// Starts at 1. Reaching the subscription's maximum dead-letters the message.
    /// </summary>
    public int DeliveryCount { get; init; }

    /// <summary>
    /// Proof that the holder owns the current peek-lock. Settlement carries it, and a stale token
    /// means the lock was lost — the message has already gone back to another receiver.
    /// </summary>
    public Guid? LockToken { get; init; }

    /// <summary>
    /// When the peek-lock expires. Past this instant the message returns to
    /// <see cref="MessageState.Available"/> and settlement with the old token fails.
    /// </summary>
    public DateTimeOffset? LockedUntil { get; init; }

    /// <summary>Current lifecycle state of this delivery.</summary>
    public MessageState State { get; init; }

    /// <summary>Set when <see cref="State"/> is <see cref="MessageState.DeadLettered"/>.</summary>
    public string? DeadLetterReason { get; init; }

    /// <summary>Free-text detail accompanying <see cref="DeadLetterReason"/>.</summary>
    public string? DeadLetterDescription { get; init; }

    /// <summary>
    /// Reads an application property, returning <c>null</c> when it is absent — the distinction
    /// between absent and null does not survive this call, so use
    /// <see cref="ApplicationProperties"/> directly where it matters.
    /// </summary>
    public object? GetProperty(string name) =>
        ApplicationProperties.TryGetValue(name, out object? value) ? value : null;

    /// <summary>Returns a read-only view of the application properties.</summary>
    public IReadOnlyDictionary<string, object?> ReadProperties() =>
        new ReadOnlyDictionary<string, object?>(
            ApplicationProperties as IDictionary<string, object?>
            ?? new Dictionary<string, object?>(ApplicationProperties, StringComparer.Ordinal));
}
