namespace PubSub.Abstractions;

/// <summary>Settings for a subscription, fixed at creation and adjustable afterwards.</summary>
public sealed class SubscriptionOptions
{
    /// <summary>
    /// How long a receiver holds a message before the lock expires and it is redelivered.
    /// </summary>
    /// <remarks>
    /// Set this above the time processing normally takes, with headroom. Too short and healthy
    /// work is redelivered as duplicates; too long and a crashed consumer's messages sit idle for
    /// the full duration before anyone else can pick them up.
    /// </remarks>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Delivery attempts allowed before the message is dead-lettered.
    /// </summary>
    /// <remarks>
    /// This is the backstop against a poison message consuming a consumer indefinitely. Note that
    /// a lock lost to expiry counts as an attempt, so a subscription whose lock duration is too
    /// short can dead-letter perfectly good messages.
    /// </remarks>
    public int MaxDeliveryCount { get; set; } = 10;

    /// <summary>
    /// Requires every message to carry a <see cref="MessageEnvelope.SessionId"/>, and delivers
    /// each session to one consumer at a time in sequence order.
    /// </summary>
    /// <remarks>
    /// Ordering is bought with throughput: a session is processed serially, so a slow message
    /// blocks the rest of its session. Enable this only where order genuinely matters, and choose
    /// a session key granular enough to keep sessions independent.
    /// </remarks>
    public bool RequiresSession { get; set; }

    /// <summary>How long a session lock is held before an idle consumer loses it.</summary>
    public TimeSpan SessionLockDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Dead-letters messages whose time to live elapses, instead of discarding them.</summary>
    public bool DeadLetterOnMessageExpiration { get; set; } = true;

    /// <summary>
    /// Dead-letters a message when a rule throws while being evaluated against it, instead of
    /// dropping it silently. Leaving this on makes filter bugs visible rather than invisible.
    /// </summary>
    public bool DeadLetterOnFilterEvaluationError { get; set; } = true;

    /// <summary>Overrides the topic's default time to live for this subscription's copies.</summary>
    public TimeSpan? DefaultTimeToLive { get; set; }

    /// <summary>Stops new deliveries while set, without deleting the subscription.</summary>
    public bool ReceivingSuspended { get; set; }
}
