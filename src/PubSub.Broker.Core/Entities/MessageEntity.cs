namespace PubSub.Broker.Core;

/// <summary>
/// A published message, stored once per topic regardless of how many subscriptions receive it.
/// </summary>
/// <remarks>
/// The payload lives here and the per-subscription delivery state lives on
/// <see cref="DeliveryEntity"/>. Copying the body once per subscription would multiply storage and
/// write cost by the fan-out factor for no benefit, since subscriptions never modify the payload —
/// only the properties, and only on their own delivery row.
/// </remarks>
public sealed class MessageEntity
{
    /// <summary>
    /// Surrogate key, and the message's sequence number.
    /// </summary>
    /// <remarks>
    /// Backed by an identity column, so it increases monotonically per topic in publish order.
    /// This is what defines delivery order within a session and what identifies a deferred message
    /// for later retrieval.
    /// </remarks>
    public long SequenceNumber { get; set; }

    /// <summary>The topic this was published to.</summary>
    public int TopicId { get; set; }

    /// <summary>Navigation to the topic.</summary>
    public TopicEntity? Topic { get; set; }

    /// <summary>Producer-assigned identity, used for duplicate detection.</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>Ties the message to a conversation.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>The message's type or label — the primary routing discriminator.</summary>
    public string? Subject { get; set; }

    /// <summary>MIME type of <see cref="Body"/>.</summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>The payload. Opaque to the broker.</summary>
    public byte[] Body { get; set; } = [];

    /// <summary>Producer-set properties, serialized as JSON.</summary>
    public string? ApplicationPropertiesJson { get; set; }

    /// <summary>Groups messages that must be processed in order.</summary>
    public string? SessionId { get; set; }

    /// <summary>Where a reply should be sent.</summary>
    public string? ReplyTo { get; set; }

    /// <summary>Session to reply on.</summary>
    public string? ReplyToSessionId { get; set; }

    /// <summary>Application-defined destination.</summary>
    public string? To { get; set; }

    /// <summary>When the broker accepted the message.</summary>
    public DateTimeOffset EnqueuedTime { get; set; }

    /// <summary>When the message stops being eligible for delivery.</summary>
    public DateTimeOffset ExpiresAt { get; set; }

    /// <summary>The deliveries fanned out from this message.</summary>
    public ICollection<DeliveryEntity> Deliveries { get; } = [];
}
