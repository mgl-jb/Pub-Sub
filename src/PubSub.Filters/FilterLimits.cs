namespace PubSub.Filters;

/// <summary>
/// Bounds on what the parser will accept.
/// </summary>
/// <remarks>
/// Filter expressions are evaluated against every message published to the topic, and in a
/// multi-tenant deployment they may be authored by someone other than the operator. These caps
/// keep a pathological expression — deep nesting, a vast <c>IN</c> list — from turning into a
/// per-message cost for everyone on the topic.
/// </remarks>
public sealed class FilterLimits
{
    /// <summary>The defaults, used when no limits are supplied.</summary>
    public static readonly FilterLimits Default = new();

    /// <summary>Longest accepted expression, in characters.</summary>
    public int MaxExpressionLength { get; init; } = 4096;

    /// <summary>Deepest accepted nesting of the parsed tree.</summary>
    public int MaxDepth { get; init; } = 32;

    /// <summary>Most values allowed in a single <c>IN</c> list.</summary>
    public int MaxInListItems { get; init; } = 128;

    /// <summary>Longest accepted property name, in characters.</summary>
    public int MaxIdentifierLength { get; init; } = 128;

    /// <summary>Longest accepted string literal, in characters.</summary>
    public int MaxStringLiteralLength { get; init; } = 1024;
}
