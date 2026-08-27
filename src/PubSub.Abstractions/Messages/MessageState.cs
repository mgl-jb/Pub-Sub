namespace PubSub.Abstractions;

/// <summary>
/// Lifecycle state of a single delivery — that is, of one message as it is routed to one
/// subscription. The same message fanned out to three subscriptions has three independent states.
/// </summary>
public enum MessageState
{
    /// <summary>Ready to be claimed by a receiver, provided its scheduled time has passed.</summary>
    Available = 0,

    /// <summary>Claimed by a receiver under a peek-lock that has not yet expired or been settled.</summary>
    Locked = 1,

    /// <summary>Settled successfully. Retained briefly for diagnostics, then pruned.</summary>
    Completed = 2,

    /// <summary>Set aside by the receiver; retrievable only by sequence number.</summary>
    Deferred = 3,

    /// <summary>Moved to the dead-letter queue. See <see cref="DeadLetterReason"/>.</summary>
    DeadLettered = 4,
}
