using PubSub.Abstractions;

namespace PubSub.Broker.Core;

/// <summary>
/// One message's journey to one subscription — the unit consumers actually claim and settle.
/// </summary>
/// <remarks>
/// <para>
/// A publish creates one row here per matching subscription. Each carries its own state, lock, and
/// delivery count, so a message that a slow subscriber is still retrying has no bearing on a
/// subscriber that already completed it.
/// </para>
/// <para>
/// <see cref="SequenceNumber"/> and <see cref="SessionId"/> are copied from the message rather
/// than joined, so the claim query — the hottest query in the system — can order and filter
/// without touching the messages table.
/// </para>
/// </remarks>
public sealed class DeliveryEntity
{
    /// <summary>Surrogate key.</summary>
    public long Id { get; set; }

    /// <summary>The message being delivered.</summary>
    public long MessageSequenceNumber { get; set; }

    /// <summary>Navigation to the message.</summary>
    public MessageEntity? Message { get; set; }

    /// <summary>The subscription this delivery belongs to.</summary>
    public int SubscriptionId { get; set; }

    /// <summary>Navigation to the subscription.</summary>
    public SubscriptionEntity? Subscription { get; set; }

    /// <summary>Copied from the message so ordering needs no join.</summary>
    public long SequenceNumber { get; set; }

    /// <summary>Copied from the message so session filtering needs no join.</summary>
    public string? SessionId { get; set; }

    /// <summary>Current lifecycle state.</summary>
    public MessageState State { get; set; }

    /// <summary>
    /// When this delivery becomes visible to receivers. In the future for a scheduled message, and
    /// pushed forward by a delayed retry.
    /// </summary>
    public DateTimeOffset AvailableAt { get; set; }

    /// <summary>How many times the delivery has been handed to a receiver.</summary>
    public int DeliveryCount { get; set; }

    /// <summary>Proof of the current peek-lock. Settlement must present a matching token.</summary>
    public Guid? LockToken { get; set; }

    /// <summary>When the current peek-lock expires.</summary>
    public DateTimeOffset? LockedUntil { get; set; }

    /// <summary>Identifies the holder of the lock, for diagnostics.</summary>
    public string? LockedBy { get; set; }

    /// <summary>Why the delivery was dead-lettered.</summary>
    public string? DeadLetterReason { get; set; }

    /// <summary>Free-text detail accompanying the dead-letter reason.</summary>
    public string? DeadLetterDescription { get; set; }

    /// <summary>
    /// Properties as this subscription sees them, when a rule action modified them.
    /// </summary>
    /// <remarks>
    /// Null means "unchanged" — the vast majority of deliveries — in which case the message's own
    /// properties are used and nothing extra is stored.
    /// </remarks>
    public string? OverriddenPropertiesJson { get; set; }

    /// <summary>When the delivery was settled, for pruning and diagnostics.</summary>
    public DateTimeOffset? SettledAt { get; set; }

    /// <summary>When this delivery's message expires, copied so expiry needs no join.</summary>
    public DateTimeOffset ExpiresAt { get; set; }

    /// <summary>When the delivery row was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }
}
