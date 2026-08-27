using System.Text.Json.Serialization;

namespace PubSub.Broker.Api;

/// <summary>A message as it crosses the wire.</summary>
/// <remarks>
/// The body travels base64-encoded because it is arbitrary bytes: the broker never parses a
/// payload, so it cannot assume the content is text, let alone valid JSON.
/// </remarks>
public sealed record MessageDto
{
    /// <summary>Producer-assigned identity, used for duplicate detection.</summary>
    public string? MessageId { get; init; }

    /// <summary>Ties the message to a conversation.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The message's type or label — the primary routing discriminator.</summary>
    public string? Subject { get; init; }

    /// <summary>MIME type of the body.</summary>
    public string ContentType { get; init; } = "application/json";

    /// <summary>The payload, base64-encoded.</summary>
    public string? Body { get; init; }

    /// <summary>Properties that subscription filters match on.</summary>
    public Dictionary<string, object?>? ApplicationProperties { get; init; }

    /// <summary>Groups messages that must be processed in order.</summary>
    public string? SessionId { get; init; }

    /// <summary>Where a reply should be sent.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Session to reply on.</summary>
    public string? ReplyToSessionId { get; init; }

    /// <summary>Application-defined destination.</summary>
    public string? To { get; init; }

    /// <summary>Withholds the message until this instant.</summary>
    public DateTimeOffset? ScheduledEnqueueTime { get; init; }

    /// <summary>How long the message stays eligible for delivery.</summary>
    public TimeSpan? TimeToLive { get; init; }

    // --- Broker-assigned; ignored on publish. ---

    /// <summary>Monotonic per topic, assigned at publish.</summary>
    public long SequenceNumber { get; init; }

    /// <summary>When the broker accepted the message.</summary>
    public DateTimeOffset EnqueuedTime { get; init; }

    /// <summary>Delivery attempts so far, including the current one.</summary>
    public int DeliveryCount { get; init; }

    /// <summary>Why the message was dead-lettered, when it was.</summary>
    public string? DeadLetterReason { get; init; }

    /// <summary>Detail accompanying the dead-letter reason.</summary>
    public string? DeadLetterDescription { get; init; }
}

/// <summary>A request to publish one or more messages.</summary>
public sealed record PublishRequestDto
{
    /// <summary>The messages to publish. The batch is atomic.</summary>
    public required IReadOnlyList<MessageDto> Messages { get; init; }
}

/// <summary>What a publish produced, per message and in the order supplied.</summary>
public sealed record PublishResponseDto
{
    /// <summary>One result per submitted message.</summary>
    public required IReadOnlyList<PublishResultDto> Results { get; init; }
}

/// <summary>The outcome for one published message.</summary>
public sealed record PublishResultDto
{
    /// <summary>The sequence number assigned, or the original's when suppressed.</summary>
    public required long SequenceNumber { get; init; }

    /// <summary>Whether duplicate detection suppressed this publish.</summary>
    public required bool WasDuplicate { get; init; }

    /// <summary>How many subscriptions the message was routed to.</summary>
    public required int MatchedSubscriptions { get; init; }
}

/// <summary>A request to receive messages.</summary>
public sealed record ReceiveRequestDto
{
    /// <summary>Most messages to claim.</summary>
    public int MaxMessages { get; init; } = 1;

    /// <summary>How long to wait for a message before returning empty.</summary>
    public TimeSpan MaxWaitTime { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Restricts the claim to one session; required on a session-enabled subscription.</summary>
    public string? SessionId { get; init; }

    /// <summary>Identifies the receiver in lock diagnostics.</summary>
    public string? ReceiverId { get; init; }
}

/// <summary>A message handed to a receiver under a peek-lock.</summary>
public sealed record ReceivedMessageDto
{
    /// <summary>The delivery's key, used to settle it.</summary>
    public required long DeliveryId { get; init; }

    /// <summary>Proof of the lock. Settlement must present this token.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>When the lock expires.</summary>
    public required DateTimeOffset LockedUntil { get; init; }

    /// <summary>The message.</summary>
    public required MessageDto Message { get; init; }
}

/// <summary>The messages a receive call claimed.</summary>
public sealed record ReceiveResponseDto
{
    /// <summary>The claimed messages, possibly empty.</summary>
    public required IReadOnlyList<ReceivedMessageDto> Messages { get; init; }
}

/// <summary>A settlement request carrying the lock token that authorises it.</summary>
public sealed record SettleRequestDto
{
    /// <summary>The lock held on the delivery.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>Properties to merge before redelivery, for abandon.</summary>
    public Dictionary<string, object?>? PropertiesToModify { get; init; }

    /// <summary>Withholds an abandoned message for this long before it becomes visible again.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>A short, filterable cause, for dead-letter.</summary>
    public string? Reason { get; init; }

    /// <summary>Free-text detail, for dead-letter.</summary>
    public string? Description { get; init; }
}

/// <summary>A request to retrieve deferred messages by sequence number.</summary>
public sealed record ReceiveDeferredRequestDto
{
    /// <summary>The sequence numbers to retrieve.</summary>
    public required IReadOnlyList<long> SequenceNumbers { get; init; }

    /// <summary>Identifies the receiver in lock diagnostics.</summary>
    public string? ReceiverId { get; init; }
}

/// <summary>A request to accept a session.</summary>
public sealed record AcceptSessionRequestDto
{
    /// <summary>The session to accept, or null for whichever is next available.</summary>
    public string? SessionId { get; init; }

    /// <summary>Identifies the holder in diagnostics.</summary>
    public string? ReceiverId { get; init; }
}

/// <summary>An accepted session.</summary>
public sealed record AcceptedSessionDto
{
    /// <summary>The session identifier.</summary>
    public required string SessionId { get; init; }

    /// <summary>Proof of ownership.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>When the session lock expires.</summary>
    public required DateTimeOffset LockedUntil { get; init; }

    /// <summary>Consumer-managed state, base64-encoded.</summary>
    public string? State { get; init; }
}

/// <summary>Session state being written.</summary>
public sealed record SessionStateDto
{
    /// <summary>Proof of ownership of the session.</summary>
    public required Guid LockToken { get; init; }

    /// <summary>The state, base64-encoded. Null clears it.</summary>
    public string? State { get; init; }
}

/// <summary>A request to replay dead-lettered messages.</summary>
public sealed record ReplayRequestDto
{
    /// <summary>Specific sequence numbers to replay, or null for the oldest available.</summary>
    public IReadOnlyList<long>? SequenceNumbers { get; init; }

    /// <summary>Most messages to replay in this call.</summary>
    public int MaxCount { get; init; } = 100;
}

/// <summary>Settings for creating a topic.</summary>
public sealed record CreateTopicDto
{
    /// <summary>Default time to live for messages that do not set their own.</summary>
    public TimeSpan? DefaultTimeToLive { get; init; }

    /// <summary>Whether repeated message ids are suppressed within the detection window.</summary>
    public bool DuplicateDetectionEnabled { get; init; }

    /// <summary>How far back duplicate detection looks.</summary>
    public TimeSpan? DuplicateDetectionWindow { get; init; }

    /// <summary>Largest accepted message body, in bytes.</summary>
    public int? MaxMessageSizeBytes { get; init; }
}

/// <summary>Settings for creating a subscription.</summary>
public sealed record CreateSubscriptionDto
{
    /// <summary>How long a receiver holds a message before the lock expires.</summary>
    public TimeSpan? LockDuration { get; init; }

    /// <summary>Delivery attempts allowed before dead-lettering.</summary>
    public int? MaxDeliveryCount { get; init; }

    /// <summary>Requires a session on every message and delivers each session in order.</summary>
    public bool RequiresSession { get; init; }

    /// <summary>How long a session lock is held before an idle consumer loses it.</summary>
    public TimeSpan? SessionLockDuration { get; init; }

    /// <summary>Dead-letters expired messages rather than discarding them.</summary>
    public bool DeadLetterOnMessageExpiration { get; init; } = true;

    /// <summary>A rule to create with the subscription; a catch-all is used when omitted.</summary>
    public CreateRuleDto? Rule { get; init; }
}

/// <summary>A rule to add to a subscription.</summary>
public sealed record CreateRuleDto
{
    /// <summary>The rule's name, unique within its subscription.</summary>
    public required string Name { get; init; }

    /// <summary>A boolean expression in the filter language.</summary>
    public string? SqlExpression { get; init; }

    /// <summary>Exact-match conditions, combined with AND.</summary>
    public CorrelationFilterDto? CorrelationFilter { get; init; }

    /// <summary>A transformation applied to matching messages.</summary>
    public string? Action { get; init; }
}

/// <summary>Exact-match filter conditions.</summary>
public sealed record CorrelationFilterDto
{
    /// <summary>Required correlation id.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Required subject.</summary>
    public string? Subject { get; init; }

    /// <summary>Required destination.</summary>
    public string? To { get; init; }

    /// <summary>Required reply address.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Required session id.</summary>
    public string? SessionId { get; init; }

    /// <summary>Required content type.</summary>
    public string? ContentType { get; init; }

    /// <summary>Application properties that must be present and equal.</summary>
    public Dictionary<string, object?>? ApplicationProperties { get; init; }
}

/// <summary>A topic as listed by the admin API.</summary>
public sealed record TopicDto
{
    /// <summary>The topic's name.</summary>
    public required string Name { get; init; }

    /// <summary>Default time to live for its messages.</summary>
    public required TimeSpan DefaultTimeToLive { get; init; }

    /// <summary>Whether duplicate detection is on.</summary>
    public required bool DuplicateDetectionEnabled { get; init; }

    /// <summary>When the topic was created.</summary>
    public required DateTimeOffset CreatedAt { get; init; }
}

/// <summary>A subscription as listed by the admin API.</summary>
public sealed record SubscriptionDto
{
    /// <summary>The subscription's name.</summary>
    public required string Name { get; init; }

    /// <summary>How long a receiver holds a message.</summary>
    public required TimeSpan LockDuration { get; init; }

    /// <summary>Delivery attempts allowed before dead-lettering.</summary>
    public required int MaxDeliveryCount { get; init; }

    /// <summary>Whether the subscription is session-enabled.</summary>
    public required bool RequiresSession { get; init; }

    /// <summary>When the subscription was created.</summary>
    public required DateTimeOffset CreatedAt { get; init; }
}

/// <summary>A rule as listed by the admin API.</summary>
public sealed record RuleDto
{
    /// <summary>The rule's name.</summary>
    public required string Name { get; init; }

    /// <summary>Which filter variant it uses.</summary>
    public required string FilterKind { get; init; }

    /// <summary>The filter, rendered as text.</summary>
    public string? Filter { get; init; }

    /// <summary>The action applied on match.</summary>
    public string? Action { get; init; }
}

/// <summary>Source-generated serialization for the broker's contracts.</summary>
/// <remarks>
/// Source generation keeps serialization reflection-free, which matters on the publish and receive
/// paths and makes the API trimmable and AOT-compatible.
/// </remarks>
[JsonSerializable(typeof(PublishRequestDto))]
[JsonSerializable(typeof(PublishResponseDto))]
[JsonSerializable(typeof(ReceiveRequestDto))]
[JsonSerializable(typeof(ReceiveResponseDto))]
[JsonSerializable(typeof(SettleRequestDto))]
[JsonSerializable(typeof(ReceiveDeferredRequestDto))]
[JsonSerializable(typeof(AcceptSessionRequestDto))]
[JsonSerializable(typeof(AcceptedSessionDto))]
[JsonSerializable(typeof(SessionStateDto))]
[JsonSerializable(typeof(ReplayRequestDto))]
[JsonSerializable(typeof(CreateTopicDto))]
[JsonSerializable(typeof(CreateSubscriptionDto))]
[JsonSerializable(typeof(CreateRuleDto))]
[JsonSerializable(typeof(IReadOnlyList<TopicDto>))]
[JsonSerializable(typeof(IReadOnlyList<SubscriptionDto>))]
[JsonSerializable(typeof(IReadOnlyList<RuleDto>))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class BrokerJsonContext : JsonSerializerContext;
