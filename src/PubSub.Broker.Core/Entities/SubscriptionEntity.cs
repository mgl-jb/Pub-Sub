namespace PubSub.Broker.Core;

/// <summary>A subscription: one independent consumer view of a topic's messages.</summary>
public sealed class SubscriptionEntity
{
    /// <summary>Surrogate key.</summary>
    public int Id { get; set; }

    /// <summary>The owning topic.</summary>
    public int TopicId { get; set; }

    /// <summary>Navigation to the owning topic.</summary>
    public TopicEntity? Topic { get; set; }

    /// <summary>The subscription's name, unique within its topic.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>How long a receiver holds a message before the lock expires.</summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Delivery attempts allowed before a message is dead-lettered.</summary>
    public int MaxDeliveryCount { get; set; } = 10;

    /// <summary>Requires a session id on every message and delivers each session in order.</summary>
    public bool RequiresSession { get; set; }

    /// <summary>How long a session lock is held before an idle consumer loses it.</summary>
    public TimeSpan SessionLockDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Dead-letters expired messages rather than discarding them.</summary>
    public bool DeadLetterOnMessageExpiration { get; set; } = true;

    /// <summary>Dead-letters a message whose rule evaluation threw.</summary>
    public bool DeadLetterOnFilterEvaluationError { get; set; } = true;

    /// <summary>Overrides the topic's default time to live for this subscription's copies.</summary>
    public TimeSpan? DefaultTimeToLive { get; set; }

    /// <summary>Stops new deliveries while set.</summary>
    public bool ReceivingSuspended { get; set; }

    /// <summary>When the subscription was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    /// Bumped whenever a rule changes, so cached compiled rule sets can be invalidated without
    /// comparing rule contents.
    /// </summary>
    public int RulesVersion { get; set; }

    /// <summary>The rules deciding which messages reach this subscription.</summary>
    public ICollection<RuleEntity> Rules { get; } = [];
}
