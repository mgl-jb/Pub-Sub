namespace PubSub.Broker.Core;

/// <summary>A topic: the publish target that messages fan out from.</summary>
public sealed class TopicEntity
{
    /// <summary>Surrogate key.</summary>
    public int Id { get; set; }

    /// <summary>The topic's name, unique across the broker.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Default time to live applied to messages that do not set their own.</summary>
    public TimeSpan DefaultTimeToLive { get; set; } = TimeSpan.FromDays(14);

    /// <summary>Whether repeated message ids are rejected within the detection window.</summary>
    public bool DuplicateDetectionEnabled { get; set; }

    /// <summary>How far back duplicate detection looks.</summary>
    public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Largest accepted message body, in bytes.</summary>
    public int MaxMessageSizeBytes { get; set; } = 256 * 1024;

    /// <summary>Rejects publishes while set, without deleting the topic.</summary>
    public bool PublishingSuspended { get; set; }

    /// <summary>When the topic was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>The subscriptions fanning out from this topic.</summary>
    public ICollection<SubscriptionEntity> Subscriptions { get; } = [];
}
