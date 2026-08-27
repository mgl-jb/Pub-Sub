namespace PubSub.Abstractions;

/// <summary>Settings for a topic, fixed at creation and adjustable afterwards.</summary>
public sealed class TopicOptions
{
    /// <summary>Default time to live for messages that do not set their own.</summary>
    public TimeSpan DefaultTimeToLive { get; set; } = TimeSpan.FromDays(14);

    /// <summary>
    /// Whether to reject a message whose <see cref="MessageEnvelope.MessageId"/> was already seen
    /// within <see cref="DuplicateDetectionWindow"/>.
    /// </summary>
    /// <remarks>
    /// This guards against a producer's retry creating a second message — it says nothing about
    /// the receive side, where redelivery can still hand the same message to a consumer twice.
    /// Idempotent handlers remain necessary either way.
    /// </remarks>
    public bool DuplicateDetectionEnabled { get; set; }

    /// <summary>
    /// How far back duplicate detection looks. Longer windows catch more duplicates and cost more
    /// storage; the window should comfortably exceed a producer's total retry span.
    /// </summary>
    public TimeSpan DuplicateDetectionWindow { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Largest accepted message body, in bytes.</summary>
    public int MaxMessageSizeBytes { get; set; } = 256 * 1024;

    /// <summary>Rejects publishes while set, without deleting the topic.</summary>
    public bool PublishingSuspended { get; set; }
}
