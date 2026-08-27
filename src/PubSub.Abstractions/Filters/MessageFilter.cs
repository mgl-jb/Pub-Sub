namespace PubSub.Abstractions;

/// <summary>
/// A subscription rule's matching condition. A message is copied to a subscription when any of
/// its rules match; a subscription with no rules receives nothing.
/// </summary>
/// <remarks>
/// This hierarchy is closed — the broker evaluates each variant differently, so a filter it does
/// not recognise cannot be routed. Derive nothing outside this assembly.
/// </remarks>
public abstract class MessageFilter
{
    private protected MessageFilter()
    {
    }

    /// <summary>A short description used in diagnostics and admin listings.</summary>
    public abstract override string ToString();
}

/// <summary>Matches every message. The default rule on a new subscription.</summary>
public sealed class TrueFilter : MessageFilter
{
    /// <summary>The single shared instance.</summary>
    public static readonly TrueFilter Instance = new();

    /// <inheritdoc />
    public override string ToString() => "1=1";
}

/// <summary>Matches nothing. Useful to disable a rule without deleting it.</summary>
public sealed class FalseFilter : MessageFilter
{
    /// <summary>The single shared instance.</summary>
    public static readonly FalseFilter Instance = new();

    /// <inheritdoc />
    public override string ToString() => "1=0";
}

/// <summary>
/// Matches on exact equality of system properties and application properties. Every non-null
/// member must match — the conditions are combined with AND.
/// </summary>
/// <remarks>
/// Prefer this over <see cref="SqlFilter"/> where it suffices: matching is a dictionary comparison
/// rather than an expression evaluation, and the common case (routing on <see cref="Subject"/>)
/// is exactly what it is built for.
/// </remarks>
public sealed class CorrelationFilter : MessageFilter
{
    /// <summary>Required value of <see cref="MessageEnvelope.CorrelationId"/>, if set.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.MessageId"/>, if set.</summary>
    public string? MessageId { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.Subject"/>, if set.</summary>
    public string? Subject { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.To"/>, if set.</summary>
    public string? To { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.ReplyTo"/>, if set.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.SessionId"/>, if set.</summary>
    public string? SessionId { get; init; }

    /// <summary>Required value of <see cref="MessageEnvelope.ContentType"/>, if set.</summary>
    public string? ContentType { get; init; }

    /// <summary>Application properties that must be present and equal.</summary>
    public IDictionary<string, object?> ApplicationProperties { get; init; } =
        new Dictionary<string, object?>(StringComparer.Ordinal);

    /// <summary>True when no condition is set, in which case the filter matches everything.</summary>
    public bool IsEmpty =>
        CorrelationId is null && MessageId is null && Subject is null && To is null
        && ReplyTo is null && SessionId is null && ContentType is null
        && ApplicationProperties.Count == 0;

    /// <inheritdoc />
    public override string ToString()
    {
        List<string> parts = [];
        if (CorrelationId is not null) { parts.Add($"sys.CorrelationId='{CorrelationId}'"); }
        if (MessageId is not null) { parts.Add($"sys.MessageId='{MessageId}'"); }
        if (Subject is not null) { parts.Add($"sys.Subject='{Subject}'"); }
        if (To is not null) { parts.Add($"sys.To='{To}'"); }
        if (ReplyTo is not null) { parts.Add($"sys.ReplyTo='{ReplyTo}'"); }
        if (SessionId is not null) { parts.Add($"sys.SessionId='{SessionId}'"); }
        if (ContentType is not null) { parts.Add($"sys.ContentType='{ContentType}'"); }
        parts.AddRange(ApplicationProperties.Select(p => $"{p.Key}='{p.Value}'"));
        return parts.Count == 0 ? "1=1" : string.Join(" AND ", parts);
    }
}

/// <summary>
/// Matches on a SQL-92-like boolean expression over the message's system and application
/// properties — for example <c>region = 'emea' AND total &gt; 500</c>.
/// </summary>
/// <remarks>
/// The expression is parsed and evaluated in process. It is never concatenated into a database
/// query, so it carries no SQL injection risk, but it is still attacker-influenced input when
/// subscriptions can be created by untrusted callers: the parser caps expression length and
/// nesting depth for that reason. See <c>docs/filter-language.md</c> for the grammar.
/// </remarks>
public sealed class SqlFilter : MessageFilter
{
    /// <summary>Creates a filter from an expression. The expression is not parsed until the broker compiles the rule.</summary>
    /// <param name="expression">A boolean expression in the subscription filter language.</param>
    public SqlFilter(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;
    }

    /// <summary>The unparsed expression text.</summary>
    public string Expression { get; }

    /// <inheritdoc />
    public override string ToString() => Expression;
}
