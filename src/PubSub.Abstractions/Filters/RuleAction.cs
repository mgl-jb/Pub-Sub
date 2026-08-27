namespace PubSub.Abstractions;

/// <summary>
/// An optional transformation applied to a message when a rule matches, letting a subscription
/// annotate what it receives without the producer knowing about it.
/// </summary>
/// <remarks>
/// The action modifies only the copy delivered to that subscription. Other subscriptions, and the
/// stored message, are unaffected.
/// </remarks>
public sealed class RuleAction
{
    /// <summary>Creates an action from one or more <c>SET</c> / <c>REMOVE</c> clauses.</summary>
    /// <param name="expression">For example <c>SET priority = 'high'; REMOVE internalTag</c>.</param>
    public RuleAction(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;
    }

    /// <summary>The unparsed action text.</summary>
    public string Expression { get; }

    /// <inheritdoc />
    public override string ToString() => Expression;
}
