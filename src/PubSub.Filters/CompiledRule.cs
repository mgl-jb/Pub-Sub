using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// A subscription rule compiled and ready to evaluate: its match predicate plus any
/// transformation applied on a match.
/// </summary>
/// <remarks>
/// Immutable and thread-safe once built, so a single instance is shared across every concurrent
/// publish on the topic.
/// </remarks>
public sealed class CompiledRule
{
    private readonly Func<MessageEnvelope, bool> _predicate;
    private readonly Action<MessageEnvelope>? _action;

    private CompiledRule(
        string name,
        Func<MessageEnvelope, bool> predicate,
        Action<MessageEnvelope>? action,
        string description)
    {
        Name = name;
        _predicate = predicate;
        _action = action;
        Description = description;
    }

    /// <summary>The rule's name, unique within its subscription.</summary>
    public string Name { get; }

    /// <summary>Human-readable form of the filter, for diagnostics and admin listings.</summary>
    public string Description { get; }

    /// <summary>Whether the rule has a transformation to apply on a match.</summary>
    public bool HasAction => _action is not null;

    /// <summary>Compiles a rule descriptor.</summary>
    /// <exception cref="FilterSyntaxException">The filter or action text is malformed.</exception>
    public static CompiledRule Compile(RuleDescriptor rule, FilterLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(rule);

        Func<MessageEnvelope, bool> predicate = FilterCompiler.Compile(rule.Filter, limits);
        Action<MessageEnvelope>? action = rule.Action is null
            ? null
            : RuleActionCompiler.Compile(rule.Action, limits);

        return new CompiledRule(rule.Name, predicate, action, rule.Filter.ToString());
    }

    /// <summary>Compiles a filter directly, without a surrounding descriptor.</summary>
    public static CompiledRule Compile(
        string name,
        MessageFilter filter,
        RuleAction? action = null,
        FilterLimits? limits = null) =>
        Compile(new RuleDescriptor(name, filter, action), limits);

    /// <summary>Whether the message matches this rule.</summary>
    public bool Matches(MessageEnvelope message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return _predicate(message);
    }

    /// <summary>
    /// Applies the rule's transformation to a message that has already matched. A no-op when the
    /// rule has no action.
    /// </summary>
    public void ApplyAction(MessageEnvelope message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _action?.Invoke(message);
    }

    /// <inheritdoc />
    public override string ToString() => $"{Name}: {Description}";
}
