namespace PubSub.Abstractions;

/// <summary>A named rule on a subscription: a filter and an optional transformation.</summary>
public sealed class RuleDescriptor
{
    /// <summary>Creates a rule.</summary>
    /// <param name="name">Unique within the subscription.</param>
    /// <param name="filter">The matching condition.</param>
    /// <param name="action">An optional transformation applied on match.</param>
    public RuleDescriptor(string name, MessageFilter filter, RuleAction? action = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(filter);
        Name = name;
        Filter = filter;
        Action = action;
    }

    /// <summary>The rule's name, unique within its subscription.</summary>
    public string Name { get; }

    /// <summary>The matching condition.</summary>
    public MessageFilter Filter { get; }

    /// <summary>The transformation applied to matching messages, if any.</summary>
    public RuleAction? Action { get; }

    /// <summary>The conventional name of the catch-all rule created with a new subscription.</summary>
    public const string DefaultRuleName = "$Default";
}
