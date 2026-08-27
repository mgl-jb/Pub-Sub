namespace PubSub.Broker.Core;

/// <summary>How a rule's condition is expressed.</summary>
public enum RuleFilterKind
{
    /// <summary>Matches every message.</summary>
    True = 0,

    /// <summary>Matches no message.</summary>
    False = 1,

    /// <summary>Exact matches on system and application properties.</summary>
    Correlation = 2,

    /// <summary>A boolean expression in the filter language.</summary>
    Sql = 3,
}

/// <summary>One rule on a subscription: a condition and an optional transformation.</summary>
public sealed class RuleEntity
{
    /// <summary>Surrogate key.</summary>
    public int Id { get; set; }

    /// <summary>The owning subscription.</summary>
    public int SubscriptionId { get; set; }

    /// <summary>Navigation to the owning subscription.</summary>
    public SubscriptionEntity? Subscription { get; set; }

    /// <summary>The rule's name, unique within its subscription.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Which filter variant this rule uses.</summary>
    public RuleFilterKind FilterKind { get; set; }

    /// <summary>The expression text, for <see cref="RuleFilterKind.Sql"/>.</summary>
    public string? SqlExpression { get; set; }

    /// <summary>The serialized correlation filter, for <see cref="RuleFilterKind.Correlation"/>.</summary>
    public string? CorrelationJson { get; set; }

    /// <summary>The action text applied to matching messages, if any.</summary>
    public string? ActionExpression { get; set; }

    /// <summary>When the rule was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }
}
