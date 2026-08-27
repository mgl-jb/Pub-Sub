namespace PubSub.Abstractions;

/// <summary>
/// Publishes messages to a topic. Registered as a singleton by the client library; safe for
/// concurrent use.
/// </summary>
public interface IEventPublisher
{
    /// <summary>
    /// Publishes a payload, serializing it and deriving <see cref="MessageEnvelope.Subject"/> from
    /// the registered type map.
    /// </summary>
    /// <param name="topic">Topic to publish to.</param>
    /// <param name="payload">The message body.</param>
    /// <param name="configure">Optional hook to set correlation id, session id, or properties.</param>
    /// <param name="cancellationToken">Cancels the send.</param>
    /// <returns>The broker-assigned sequence number.</returns>
    Task<long> PublishAsync<T>(
        string topic,
        T payload,
        Action<PublishOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where T : notnull;

    /// <summary>Publishes a pre-built envelope, for callers that manage their own serialization.</summary>
    /// <returns>The broker-assigned sequence number.</returns>
    Task<long> PublishAsync(
        string topic,
        MessageEnvelope message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes several messages in one request.
    /// </summary>
    /// <remarks>
    /// The batch is atomic: either every message is stored or none is. Batching cuts round trips
    /// substantially, so prefer it when publishing more than a couple of messages at once.
    /// </remarks>
    /// <returns>Sequence numbers in the order the messages were supplied.</returns>
    Task<IReadOnlyList<long>> PublishBatchAsync(
        string topic,
        IEnumerable<MessageEnvelope> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message that stays invisible to receivers until <paramref name="enqueueAt"/>.
    /// </summary>
    /// <returns>
    /// The sequence number, which is also the handle needed to cancel the message before it
    /// becomes visible.
    /// </returns>
    Task<long> ScheduleAsync(
        string topic,
        MessageEnvelope message,
        DateTimeOffset enqueueAt,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a scheduled message that has not yet become visible.
    /// </summary>
    /// <returns>
    /// <c>true</c> if it was cancelled; <c>false</c> if it had already been enqueued or did not
    /// exist. A <c>false</c> result is not an error — it means the race was lost.
    /// </returns>
    Task<bool> CancelScheduledAsync(
        string topic,
        long sequenceNumber,
        CancellationToken cancellationToken = default);
}

/// <summary>Per-message options for the generic <see cref="IEventPublisher.PublishAsync{T}"/> overload.</summary>
public sealed class PublishOptions
{
    /// <summary>Overrides the generated message id. Set this to make retried sends deduplicate.</summary>
    public string? MessageId { get; set; }

    /// <summary>Correlation id to stamp on the message.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>Overrides the subject derived from the payload's type.</summary>
    public string? Subject { get; set; }

    /// <summary>Groups the message into an ordered session.</summary>
    public string? SessionId { get; set; }

    /// <summary>Reply address for request/reply exchanges.</summary>
    public string? ReplyTo { get; set; }

    /// <summary>Reply session for request/reply exchanges.</summary>
    public string? ReplyToSessionId { get; set; }

    /// <summary>Application-defined destination.</summary>
    public string? To { get; set; }

    /// <summary>Withholds the message until this instant.</summary>
    public DateTimeOffset? ScheduledEnqueueTime { get; set; }

    /// <summary>How long the message stays eligible for delivery.</summary>
    public TimeSpan? TimeToLive { get; set; }

    /// <summary>Properties that subscription filters can match on.</summary>
    public IDictionary<string, object?> ApplicationProperties { get; } =
        new Dictionary<string, object?>(StringComparer.Ordinal);
}
