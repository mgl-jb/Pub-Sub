using PubSub.Abstractions;

namespace PubSub.Broker.Core;

/// <summary>The outcome of publishing one message.</summary>
/// <param name="SequenceNumber">The sequence number assigned, or the original's when suppressed.</param>
/// <param name="WasDuplicate">
/// True when duplicate detection suppressed the publish because the same message id was seen
/// within the topic's detection window. The message was not stored again.
/// </param>
/// <param name="MatchedSubscriptions">
/// How many subscriptions the message was routed to. Zero is legitimate — no rule matched — but a
/// persistent zero usually means a filter is wrong.
/// </param>
public readonly record struct PublishResult(
    long SequenceNumber,
    bool WasDuplicate,
    int MatchedSubscriptions);

/// <summary>A message handed to a receiver under a peek-lock.</summary>
public sealed class ReceivedMessage
{
    /// <summary>The delivery row's key, used to settle.</summary>
    public required long DeliveryId { get; init; }

    /// <summary>Proof of the peek-lock. Settlement must present this token.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>When the lock expires.</summary>
    public required DateTimeOffset LockedUntil { get; init; }

    /// <summary>The message, with delivery state filled in.</summary>
    public required MessageEnvelope Message { get; init; }
}

/// <summary>How a receive call should behave.</summary>
public sealed class ReceiveRequest
{
    /// <summary>The topic to receive from.</summary>
    public required string Topic { get; init; }

    /// <summary>The subscription to receive from.</summary>
    public required string Subscription { get; init; }

    /// <summary>Most messages to claim in this call.</summary>
    public int MaxMessages { get; init; } = 1;

    /// <summary>
    /// How long to wait for a message before returning empty. Zero returns immediately.
    /// </summary>
    public TimeSpan MaxWaitTime { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Restricts the claim to one session. Required when the subscription is session-enabled;
    /// the caller must already hold the session lock.
    /// </summary>
    public string? SessionId { get; init; }

    /// <summary>Identifies the receiver in lock diagnostics.</summary>
    public string? ReceiverId { get; init; }

    /// <summary>Receives from the subscription's dead-letter queue instead of its live messages.</summary>
    public bool FromDeadLetterQueue { get; init; }
}

/// <summary>The outcome of a settlement attempt.</summary>
public enum SettlementResult
{
    /// <summary>The delivery was settled as requested.</summary>
    Settled,

    /// <summary>
    /// The lock had expired or been taken over, so nothing was changed. The message has returned
    /// to the subscription and may already have been redelivered.
    /// </summary>
    LockLost,

    /// <summary>No delivery with that identifier exists.</summary>
    NotFound,
}

/// <summary>A session claimed by a receiver.</summary>
public sealed class AcceptedSession
{
    /// <summary>The session identifier.</summary>
    public required string SessionId { get; init; }

    /// <summary>Proof of ownership of the session.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>When the session lock expires.</summary>
    public required DateTimeOffset LockedUntil { get; init; }

    /// <summary>Consumer-managed state carried across the session.</summary>
    public byte[]? State { get; init; }
}
