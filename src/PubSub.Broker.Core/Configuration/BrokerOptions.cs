namespace PubSub.Broker.Core;

/// <summary>Broker-wide tuning knobs.</summary>
public sealed class BrokerOptions
{
    /// <summary>Configuration section these bind from.</summary>
    public const string SectionName = "Broker";

    /// <summary>Most messages a single receive call may claim.</summary>
    public int MaxReceiveBatchSize { get; set; } = 100;

    /// <summary>
    /// Longest a receive call will wait for a message before returning empty.
    /// </summary>
    /// <remarks>
    /// Long-polling trades an idle connection for latency: without it a receiver either polls
    /// tightly, burning queries, or polls slowly and adds delay to every message.
    /// </remarks>
    public TimeSpan MaxLongPollDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often a waiting receiver re-checks the database when no wakeup arrives.
    /// </summary>
    /// <remarks>
    /// This is the fallback path. With Redis connected a publish signals waiting receivers
    /// immediately and this interval rarely elapses; without it, this interval alone determines
    /// dispatch latency.
    /// </remarks>
    public TimeSpan LongPollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>How often the sweeper runs.</summary>
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Most rows the sweeper touches per statement, to keep transactions short.</summary>
    public int SweepBatchSize { get; set; } = 500;

    /// <summary>How long settled deliveries are retained before pruning.</summary>
    /// <remarks>Kept briefly so operators can confirm a message really was processed.</remarks>
    public TimeSpan CompletedRetention { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Largest accepted message body, in bytes, unless the topic overrides it.</summary>
    public int MaxMessageSizeBytes { get; set; } = 256 * 1024;

    /// <summary>Most messages accepted in a single publish batch.</summary>
    public int MaxBatchPublishCount { get; set; } = 500;

    /// <summary>Command timeout for broker database calls.</summary>
    public TimeSpan DatabaseCommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
