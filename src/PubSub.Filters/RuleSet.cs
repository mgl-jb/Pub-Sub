using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// The compiled rules of one subscription, evaluated together to decide whether a message is
/// routed to it.
/// </summary>
/// <remarks>
/// Rules are combined with OR: a message matching any rule is delivered once, even when several
/// rules match. Where more than one matching rule carries an action, only the first match's action
/// is applied, so that the delivered message is a function of the rule set rather than of
/// evaluation order.
/// </remarks>
public sealed class RuleSet
{
    private readonly CompiledRule[] _rules;

    /// <summary>Creates a rule set from already-compiled rules.</summary>
    public RuleSet(IEnumerable<CompiledRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        _rules = [.. rules];
    }

    /// <summary>Compiles a set of rule descriptors.</summary>
    public static RuleSet Compile(IEnumerable<RuleDescriptor> rules, FilterLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(rules);
        return new RuleSet(rules.Select(r => CompiledRule.Compile(r, limits)));
    }

    /// <summary>The compiled rules, in evaluation order.</summary>
    public IReadOnlyList<CompiledRule> Rules => _rules;

    /// <summary>
    /// A subscription with no rules receives nothing. This is deliberate: an empty rule set is
    /// almost always a misconfiguration, and silently delivering everything would hide it.
    /// </summary>
    public bool IsEmpty => _rules.Length == 0;

    /// <summary>
    /// Evaluates the message against the set.
    /// </summary>
    /// <param name="message">The message to route.</param>
    /// <param name="matched">The first rule that matched, if any.</param>
    /// <returns><c>true</c> when the message should be delivered to this subscription.</returns>
    public bool TryMatch(MessageEnvelope message, out CompiledRule? matched)
    {
        ArgumentNullException.ThrowIfNull(message);

        foreach (CompiledRule rule in _rules)
        {
            if (rule.Matches(message))
            {
                matched = rule;
                return true;
            }
        }

        matched = null;
        return false;
    }

    /// <summary>Whether the message matches any rule in the set.</summary>
    public bool Matches(MessageEnvelope message) => TryMatch(message, out _);
}
