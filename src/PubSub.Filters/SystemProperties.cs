using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// The built-in message properties a filter can read via the <c>sys.</c> prefix.
/// </summary>
/// <remarks>
/// Deliberately a closed set. Rejecting an unknown <c>sys.</c> name at parse time turns a typo
/// into an error when the rule is created, rather than a subscription that silently receives
/// nothing in production.
/// </remarks>
public static class SystemProperties
{
    /// <summary><see cref="MessageEnvelope.MessageId"/>.</summary>
    public const string MessageId = nameof(MessageEnvelope.MessageId);

    /// <summary><see cref="MessageEnvelope.CorrelationId"/>.</summary>
    public const string CorrelationId = nameof(MessageEnvelope.CorrelationId);

    /// <summary><see cref="MessageEnvelope.Subject"/>.</summary>
    public const string Subject = nameof(MessageEnvelope.Subject);

    /// <summary><see cref="MessageEnvelope.ContentType"/>.</summary>
    public const string ContentType = nameof(MessageEnvelope.ContentType);

    /// <summary><see cref="MessageEnvelope.SessionId"/>.</summary>
    public const string SessionId = nameof(MessageEnvelope.SessionId);

    /// <summary><see cref="MessageEnvelope.ReplyTo"/>.</summary>
    public const string ReplyTo = nameof(MessageEnvelope.ReplyTo);

    /// <summary><see cref="MessageEnvelope.ReplyToSessionId"/>.</summary>
    public const string ReplyToSessionId = nameof(MessageEnvelope.ReplyToSessionId);

    /// <summary><see cref="MessageEnvelope.To"/>.</summary>
    public const string To = nameof(MessageEnvelope.To);

    /// <summary><see cref="MessageEnvelope.EnqueuedTime"/>.</summary>
    public const string EnqueuedTime = nameof(MessageEnvelope.EnqueuedTime);

    /// <summary><see cref="MessageEnvelope.SequenceNumber"/>.</summary>
    public const string SequenceNumber = nameof(MessageEnvelope.SequenceNumber);

    /// <summary><see cref="MessageEnvelope.DeliveryCount"/>.</summary>
    public const string DeliveryCount = nameof(MessageEnvelope.DeliveryCount);

    private static readonly Dictionary<string, string> Canonical =
        new(StringComparer.OrdinalIgnoreCase)
        {
            [MessageId] = MessageId,
            [CorrelationId] = CorrelationId,
            [Subject] = Subject,
            [ContentType] = ContentType,
            [SessionId] = SessionId,
            [ReplyTo] = ReplyTo,
            [ReplyToSessionId] = ReplyToSessionId,
            [To] = To,
            [EnqueuedTime] = EnqueuedTime,
            [SequenceNumber] = SequenceNumber,
            [DeliveryCount] = DeliveryCount,

            // Accepted aliases, so a rule written against the wire format still parses.
            ["Label"] = Subject,
            ["MessageType"] = Subject,
        };

    /// <summary>Every canonical system property name.</summary>
    public static IReadOnlyCollection<string> Names { get; } =
    [
        MessageId, CorrelationId, Subject, ContentType, SessionId,
        ReplyTo, ReplyToSessionId, To, EnqueuedTime, SequenceNumber, DeliveryCount,
    ];

    /// <summary>Whether <paramref name="name"/> names a system property, ignoring case.</summary>
    public static bool IsKnown(string name) => Canonical.ContainsKey(name);

    /// <summary>Maps a name or alias to its canonical spelling.</summary>
    public static string Normalize(string name) =>
        Canonical.TryGetValue(name, out string? canonical) ? canonical : name;

    /// <summary>Reads a system property off a message.</summary>
    public static object? Read(MessageEnvelope message, string canonicalName) => canonicalName switch
    {
        MessageId => message.MessageId,
        CorrelationId => message.CorrelationId,
        Subject => message.Subject,
        ContentType => message.ContentType,
        SessionId => message.SessionId,
        ReplyTo => message.ReplyTo,
        ReplyToSessionId => message.ReplyToSessionId,
        To => message.To,
        EnqueuedTime => message.EnqueuedTime,
        SequenceNumber => message.SequenceNumber,
        DeliveryCount => message.DeliveryCount,
        _ => null,
    };
}
