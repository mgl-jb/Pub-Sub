using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// Compiles a rule action — one or more <c>SET</c> / <c>REMOVE</c> clauses separated by
/// semicolons — into a delegate that rewrites a matching message's application properties.
/// </summary>
/// <remarks>
/// The action mutates only the copy delivered to the rule's own subscription. This lets a
/// subscription annotate what it receives (a routing hint, a priority) without the producer
/// knowing, and without disturbing what other subscriptions see.
/// </remarks>
public static class RuleActionCompiler
{
    /// <summary>Compiles action text into a mutation applied to a matching message.</summary>
    /// <exception cref="FilterSyntaxException">The action text is malformed.</exception>
    public static Action<MessageEnvelope> Compile(string actionText, FilterLimits? limits = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actionText);
        limits ??= FilterLimits.Default;

        List<Action<MessageEnvelope>> steps = [];

        foreach (string clause in SplitClauses(actionText))
        {
            steps.Add(CompileClause(clause, limits));
        }

        if (steps.Count == 0)
        {
            throw new FilterSyntaxException("The action contains no SET or REMOVE clauses.", 0);
        }

        Action<MessageEnvelope>[] compiled = [.. steps];
        return message =>
        {
            foreach (Action<MessageEnvelope> step in compiled)
            {
                step(message);
            }
        };
    }

    /// <summary>Compiles a <see cref="RuleAction"/>.</summary>
    public static Action<MessageEnvelope> Compile(RuleAction action, FilterLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(action);
        return Compile(action.Expression, limits);
    }

    private static string[] SplitClauses(string text) =>
        text.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static Action<MessageEnvelope> CompileClause(string clause, FilterLimits limits)
    {
        if (clause.StartsWith("SET ", StringComparison.OrdinalIgnoreCase))
        {
            string assignment = clause[4..];
            int equals = IndexOfTopLevelEquals(assignment);
            if (equals < 0)
            {
                throw new FilterSyntaxException($"SET clause '{clause}' is missing '='.", 0);
            }

            string name = assignment[..equals].Trim();
            string valueExpression = assignment[(equals + 1)..].Trim();

            ValidatePropertyName(name, clause);

            if (valueExpression.Length == 0)
            {
                throw new FilterSyntaxException($"SET clause '{clause}' has no value expression.", 0);
            }

            Func<MessageEnvelope, object?> evaluate =
                CompileValueExpression(valueExpression, limits);

            return message => message.ApplicationProperties[name] = evaluate(message);
        }

        if (clause.StartsWith("REMOVE ", StringComparison.OrdinalIgnoreCase))
        {
            string name = clause[7..].Trim();
            ValidatePropertyName(name, clause);
            return message => message.ApplicationProperties.Remove(name);
        }

        throw new FilterSyntaxException(
            $"'{clause}' is not a valid action. Expected 'SET <property> = <expression>' or " +
            "'REMOVE <property>'.",
            0);
    }

    /// <summary>
    /// Finds the assignment '=' while skipping any that sit inside a string literal or a
    /// comparison operator (<c>&lt;=</c>, <c>&gt;=</c>, <c>&lt;&gt;</c>, <c>!=</c>).
    /// </summary>
    private static int IndexOfTopLevelEquals(string text)
    {
        bool inString = false;

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];

            if (c == '\'')
            {
                // A doubled quote inside a string is an escaped quote, not a terminator.
                if (inString && i + 1 < text.Length && text[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                inString = !inString;
                continue;
            }

            if (inString || c != '=')
            {
                continue;
            }

            if (i > 0 && text[i - 1] is '<' or '>' or '!')
            {
                continue;
            }

            return i;
        }

        return -1;
    }

    private static void ValidatePropertyName(string name, string clause)
    {
        if (name.Length == 0)
        {
            throw new FilterSyntaxException($"'{clause}' does not name a property.", 0);
        }

        if (name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase))
        {
            throw new FilterSyntaxException(
                "A rule action cannot modify a system property; only application properties are writable.",
                0);
        }

        bool valid = (char.IsLetter(name[0]) || name[0] == '_')
                     && name.All(c => char.IsLetterOrDigit(c) || c == '_');

        if (!valid)
        {
            throw new FilterSyntaxException(
                $"'{name}' is not a valid property name. Use letters, digits, and underscores, " +
                "starting with a letter or underscore.",
                0);
        }
    }

    private static Func<MessageEnvelope, object?> CompileValueExpression(
        string expression,
        FilterLimits limits)
    {
        FilterExpression parsed = FilterParser.Parse(expression, limits);

        // The filter compiler yields predicates; for an assignment we need the raw value, so the
        // expression is wrapped to surface whatever it evaluated to.
        return ValueEvaluator.Build(parsed);
    }
}
