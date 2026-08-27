namespace PubSub.Broker.Core;

/// <summary>
/// A record that a message id was published to a topic, used for duplicate detection.
/// </summary>
/// <remarks>
/// This suppresses duplicates created by a producer's retry — a send that timed out but had
/// actually succeeded. It says nothing about the receive side: redelivery can still hand the same
/// message to a consumer twice, so idempotent handlers remain necessary regardless.
/// </remarks>
public sealed class DedupEntity
{
    /// <summary>Surrogate key.</summary>
    public long Id { get; set; }

    /// <summary>The topic the message was published to.</summary>
    public int TopicId { get; set; }

    /// <summary>The producer-assigned message id.</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>The sequence number assigned to the original publish.</summary>
    public long SequenceNumber { get; set; }

    /// <summary>When the original publish happened.</summary>
    public DateTimeOffset PublishedAt { get; set; }

    /// <summary>
    /// When this record stops suppressing duplicates and becomes eligible for pruning.
    /// </summary>
    public DateTimeOffset ExpiresAt { get; set; }
}
