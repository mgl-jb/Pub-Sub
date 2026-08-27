namespace PubSub.Outbox;

/// <summary>Where an outbox message is in its journey to the broker.</summary>
public enum OutboxStatus
{
    /// <summary>Waiting to be published.</summary>
    Pending = 0,

    /// <summary>Claimed by a publisher instance and being sent.</summary>
    InFlight = 1,

    /// <summary>Accepted by the broker.</summary>
    Published = 2,

    /// <summary>Abandoned after too many failed attempts; needs an operator.</summary>
    Failed = 3,
}

/// <summary>
/// A message the application intends to publish, written in the same transaction as the data
/// change that motivated it.
/// </summary>
/// <remarks>
/// <para>
/// This exists because a database write and a broker publish cannot be made atomic. Publishing
/// first risks announcing something that never got saved; saving first risks a crash before the
/// publish, so the change happens and nobody hears about it. Neither ordering is safe, and the
/// window is small enough to look fine in testing and bite in production.
/// </para>
/// <para>
/// Writing the intent to the same database in the same transaction removes the choice: either both
/// land or neither does. A background publisher then moves it to the broker, which turns an
/// impossible atomicity problem into an ordinary at-least-once one — the publisher may send twice,
/// which duplicate detection and idempotent consumers already handle.
/// </para>
/// </remarks>
public sealed class OutboxMessage
{
    /// <summary>Surrogate key, also the publish order.</summary>
    public long Id { get; set; }

    /// <summary>The topic to publish to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Producer-assigned identity, carried through so duplicate detection can use it.</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>The message's type or label.</summary>
    public string? Subject { get; set; }

    /// <summary>Ties the message to a conversation.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>Groups the message into an ordered session.</summary>
    public string? SessionId { get; set; }

    /// <summary>MIME type of the body.</summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>The serialized payload.</summary>
    public byte[] Body { get; set; } = [];

    /// <summary>Properties subscription filters match on, serialized as JSON.</summary>
    public string? ApplicationPropertiesJson { get; set; }

    /// <summary>Withholds the message until this instant once published.</summary>
    public DateTimeOffset? ScheduledEnqueueTime { get; set; }

    /// <summary>Current status.</summary>
    public OutboxStatus Status { get; set; }

    /// <summary>How many publish attempts have been made.</summary>
    public int AttemptCount { get; set; }

    /// <summary>When the next attempt may be made; backs off after a failure.</summary>
    public DateTimeOffset NextAttemptAt { get; set; }

    /// <summary>The last failure's message, for diagnosis.</summary>
    public string? LastError { get; set; }

    /// <summary>Identifies the publisher instance currently holding this row.</summary>
    public string? ClaimedBy { get; set; }

    /// <summary>When the current claim expires, so a crashed publisher does not strand the row.</summary>
    public DateTimeOffset? ClaimedUntil { get; set; }

    /// <summary>When the intent was written.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>When the broker accepted it.</summary>
    public DateTimeOffset? PublishedAt { get; set; }
}

/// <summary>
/// A record that a message has already been processed, so a redelivery can be recognised.
/// </summary>
/// <remarks>
/// The marker is written in the same transaction as the business change it accompanies. That is
/// what makes it trustworthy: a crash between doing the work and recording that it was done would
/// otherwise leave the two out of step, and the work would repeat.
/// </remarks>
public sealed class InboxMessage
{
    /// <summary>Surrogate key.</summary>
    public long Id { get; set; }

    /// <summary>The message's producer-assigned identity.</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>
    /// Which consumer processed it.
    /// </summary>
    /// <remarks>
    /// Part of the key because the same message legitimately reaches several subscriptions, and
    /// one having processed it says nothing about the others.
    /// </remarks>
    public string Consumer { get; set; } = string.Empty;

    /// <summary>When it was processed.</summary>
    public DateTimeOffset ProcessedAt { get; set; }

    /// <summary>
    /// When this record may be pruned.
    /// </summary>
    /// <remarks>
    /// It must outlive every way the message could come back — the full retry budget, the
    /// dead-letter queue, and an operator replaying it days later. Pruning early reopens the
    /// duplicate window silently.
    /// </remarks>
    public DateTimeOffset ExpiresAt { get; set; }
}
