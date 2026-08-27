namespace PubSub.Broker.Core;

/// <summary>
/// Exclusive ownership of one session on one subscription.
/// </summary>
/// <remarks>
/// Ordering guarantees come from exclusivity: only the holder of this lock may claim that
/// session's deliveries, so its messages are processed one at a time, in sequence order. Without
/// the lock, two consumers could take adjacent messages from the same session concurrently and
/// finish them out of order.
/// </remarks>
public sealed class SessionLockEntity
{
    /// <summary>Surrogate key.</summary>
    public long Id { get; set; }

    /// <summary>The subscription the session belongs to.</summary>
    public int SubscriptionId { get; set; }

    /// <summary>Navigation to the subscription.</summary>
    public SubscriptionEntity? Subscription { get; set; }

    /// <summary>The session identifier.</summary>
    public string SessionId { get; set; } = string.Empty;

    /// <summary>Proof of ownership. Session operations must present a matching token.</summary>
    public Guid LockToken { get; set; }

    /// <summary>When the session lock expires.</summary>
    public DateTimeOffset LockedUntil { get; set; }

    /// <summary>Identifies the holder, for diagnostics.</summary>
    public string? LockedBy { get; set; }

    /// <summary>
    /// Consumer-managed state carried across the session, letting a handler checkpoint progress
    /// without a separate store.
    /// </summary>
    public byte[]? State { get; set; }

    /// <summary>When the session was last accepted or renewed.</summary>
    public DateTimeOffset AcquiredAt { get; set; }
}
