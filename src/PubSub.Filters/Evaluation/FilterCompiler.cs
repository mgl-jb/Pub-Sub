using System.Text.RegularExpressions;
using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// Compiles a parsed filter into a delegate that evaluates it against a message.
/// </summary>
/// <remarks>
/// <para>
/// Compilation happens once per rule; the resulting delegate then runs against every message
/// published to the topic, so the cost model is "compile rarely, evaluate constantly".
/// </para>
/// <para>
/// The technique is closure compilation: each node becomes a small delegate that closes over its
/// children's delegates, so evaluation is a chain of direct calls with no per-message tree walking,
/// no dictionary lookups for operators, and no re-parsing. It is chosen over
/// <see cref="System.Linq.Expressions"/> deliberately — it needs no runtime code generation, so it
/// works unchanged under Native AOT and on runtimes where dynamic IL emission is unavailable,
/// while performing comparably. Regexes for <c>LIKE</c> and the frozen value sets for <c>IN</c>
/// are built at compile time for the same reason.
/// </para>
/// </remarks>
public static class FilterCompiler
{
    /// <summary>
    /// Compiles an expression tree into a predicate.
    /// </summary>
    /// <remarks>
    /// Only TRUE matches. A rule that evaluates to FALSE or to UNKNOWN does not route the message —
    /// so a filter referencing a property the message lacks simply does not match, rather than
    /// failing.
    /// </remarks>
    public static Func<MessageEnvelope, bool> Compile(FilterExpression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        Func<MessageEnvelope, object?> evaluator = CompileNode(expression);
        return message => SqlValue.AsBoolean(evaluator(message)) == true;
    }

    /// <summary>Parses and compiles in one step.</summary>
    /// <exception cref="FilterSyntaxException">The expression is malformed.</exception>
    public static Func<MessageEnvelope, bool> Compile(string expression, FilterLimits? limits = null) =>
        Compile(FilterParser.Parse(expression, limits));

    /// <summary>Compiles a <see cref="MessageFilter"/> of any variant into a predicate.</summary>
    public static Func<MessageEnvelope, bool> Compile(MessageFilter filter, FilterLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(filter);

        return filter switch
        {
            TrueFilter => static _ => true,
            FalseFilter => static _ => false,
            SqlFilter sql => Compile(sql.Expression, limits),
            CorrelationFilter correlation => CompileCorrelation(correlation),
            _ => throw new NotSupportedException(
                $"Filter type '{filter.GetType().Name}' is not supported."),
        };
    }

    /// <summary>
    /// Compiles a correlation filter into a chain of equality checks.
    /// </summary>
    /// <remarks>
    /// Kept separate from the expression path on purpose: this is the common case, and a short
    /// list of ordinal string comparisons beats routing it through the general evaluator.
    /// </remarks>
    private static Func<MessageEnvelope, bool> CompileCorrelation(CorrelationFilter filter)
    {
        if (filter.IsEmpty)
        {
            return static _ => true;
        }

        List<Func<MessageEnvelope, bool>> checks = [];

        if (filter.CorrelationId is { } correlationId)
        {
            checks.Add(m => string.Equals(m.CorrelationId, correlationId, StringComparison.Ordinal));
        }

        if (filter.MessageId is { } messageId)
        {
            checks.Add(m => string.Equals(m.MessageId, messageId, StringComparison.Ordinal));
        }

        if (filter.Subject is { } subject)
        {
            checks.Add(m => string.Equals(m.Subject, subject, StringComparison.Ordinal));
        }

        if (filter.To is { } to)
        {
            checks.Add(m => string.Equals(m.To, to, StringComparison.Ordinal));
        }

        if (filter.ReplyTo is { } replyTo)
        {
            checks.Add(m => string.Equals(m.ReplyTo, replyTo, StringComparison.Ordinal));
        }

        if (filter.SessionId is { } sessionId)
        {
            checks.Add(m => string.Equals(m.SessionId, sessionId, StringComparison.Ordinal));
        }

        if (filter.ContentType is { } contentType)
        {
            checks.Add(m => string.Equals(m.ContentType, contentType, StringComparison.Ordinal));
        }

        foreach (KeyValuePair<string, object?> property in filter.ApplicationProperties)
        {
            string name = property.Key;
            object? expected = property.Value;

            // A correlation filter tests for equality, so a property that is absent does not match —
            // even when the expected value is null. EXISTS is the way to ask about presence.
            checks.Add(m => m.ApplicationProperties.TryGetValue(name, out object? actual)
                            && SqlValue.AreEqual(actual, expected) == true);
        }

        Func<MessageEnvelope, bool>[] compiled = [.. checks];
        return message =>
        {
            foreach (Func<MessageEnvelope, bool> check in compiled)
            {
                if (!check(message))
                {
                    return false;
                }
            }

            return true;
        };
    }

    /// <summary>
    /// Compiles an expression into a delegate yielding its value rather than a match decision.
    /// </summary>
    /// <remarks>
    /// Rule actions need this: <c>SET total = price * quantity</c> assigns a number, where the
    /// predicate path would have collapsed it to a boolean.
    /// </remarks>
    public static Func<MessageEnvelope, object?> CompileValue(FilterExpression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return CompileNode(expression);
    }

    private static Func<MessageEnvelope, object?> CompileNode(FilterExpression expression) => expression switch
    {
        LiteralExpression literal => CompileLiteral(literal),
        PropertyExpression property => CompileProperty(property),
        UnaryExpression unary => CompileUnary(unary),
        BinaryExpression binary => CompileBinary(binary),
        LikeExpression like => CompileLike(like),
        InExpression inExpression => CompileIn(inExpression),
        IsNullExpression isNull => CompileIsNull(isNull),
        ExistsExpression exists => CompileExists(exists),
        _ => throw new NotSupportedException(
            $"Expression node '{expression.GetType().Name}' is not supported."),
    };

    private static Func<MessageEnvelope, object?> CompileLiteral(LiteralExpression literal)
    {
        object? value = literal.Value;
        return _ => value;
    }

    private static Func<MessageEnvelope, object?> CompileProperty(PropertyExpression property)
    {
        string name = property.Name;

        if (property.Source == PropertySource.System)
        {
            return message => SystemProperties.Read(message, name);
        }

        return message =>
            message.ApplicationProperties.TryGetValue(name, out object? value) ? value : null;
    }

    private static Func<MessageEnvelope, object?> CompileUnary(UnaryExpression unary)
    {
        Func<MessageEnvelope, object?> operand = CompileNode(unary.Operand);

        return unary.Operator switch
        {
            UnaryOperator.Not => message => SqlValue.Not(SqlValue.AsBoolean(operand(message))),
            UnaryOperator.Negate => message => SqlValue.Negate(operand(message)),
            _ => throw new NotSupportedException($"Unary operator '{unary.Operator}' is not supported."),
        };
    }

    private static Func<MessageEnvelope, object?> CompileBinary(BinaryExpression binary)
    {
        Func<MessageEnvelope, object?> left = CompileNode(binary.Left);
        Func<MessageEnvelope, object?> right = CompileNode(binary.Right);

        switch (binary.Operator)
        {
            // AND and OR short-circuit only where three-valued logic allows it: a FALSE on the left
            // settles AND, and a TRUE on the left settles OR, whatever the right side evaluates to.
            case BinaryOperator.And:
                return message =>
                {
                    bool? l = SqlValue.AsBoolean(left(message));
                    if (l == false)
                    {
                        return false;
                    }

                    return SqlValue.And(l, SqlValue.AsBoolean(right(message)));
                };

            case BinaryOperator.Or:
                return message =>
                {
                    bool? l = SqlValue.AsBoolean(left(message));
                    if (l == true)
                    {
                        return true;
                    }

                    return SqlValue.Or(l, SqlValue.AsBoolean(right(message)));
                };

            case BinaryOperator.Equal:
                return message => SqlValue.AreEqual(left(message), right(message));

            case BinaryOperator.NotEqual:
                return message => SqlValue.Not(SqlValue.AreEqual(left(message), right(message)));

            case BinaryOperator.LessThan:
                return message => CompareTo(left(message), right(message), static c => c < 0);

            case BinaryOperator.LessThanOrEqual:
                return message => CompareTo(left(message), right(message), static c => c <= 0);

            case BinaryOperator.GreaterThan:
                return message => CompareTo(left(message), right(message), static c => c > 0);

            case BinaryOperator.GreaterThanOrEqual:
                return message => CompareTo(left(message), right(message), static c => c >= 0);

            case BinaryOperator.Add:
            case BinaryOperator.Subtract:
            case BinaryOperator.Multiply:
            case BinaryOperator.Divide:
            case BinaryOperator.Modulo:
                {
                    BinaryOperator op = binary.Operator;
                    return message => SqlValue.Arithmetic(op, left(message), right(message));
                }

            default:
                throw new NotSupportedException($"Binary operator '{binary.Operator}' is not supported.");
        }
    }

    /// <summary>
    /// Applies an ordering predicate to a comparison, preserving UNKNOWN as <c>null</c>.
    /// </summary>
    private static bool? CompareTo(object? left, object? right, Func<int, bool> predicate)
    {
        int? comparison = SqlValue.Compare(left, right);
        return comparison is null ? null : predicate(comparison.Value);
    }

    private static Func<MessageEnvelope, object?> CompileLike(LikeExpression like)
    {
        Func<MessageEnvelope, object?> value = CompileNode(like.Value);
        Regex regex = LikePattern.Compile(like.Pattern, like.Escape);
        bool negated = like.Negated;

        return message =>
        {
            string? text = SqlValue.AsString(value(message));
            if (text is null)
            {
                // Null, or a non-string value: UNKNOWN either way.
                return null;
            }

            try
            {
                bool matched = regex.IsMatch(text);
                return negated ? !matched : matched;
            }
            catch (RegexMatchTimeoutException)
            {
                // Treated as UNKNOWN so one pathological value cannot fail a publish.
                return null;
            }
        };
    }

    private static Func<MessageEnvelope, object?> CompileIn(InExpression expression)
    {
        Func<MessageEnvelope, object?> value = CompileNode(expression.Value);
        Func<MessageEnvelope, object?>[] items = [.. expression.Items.Select(CompileNode)];
        bool negated = expression.Negated;

        return message =>
        {
            object? subject = value(message);
            if (subject is null)
            {
                return null;
            }

            // SQL semantics: a non-match is only definitive once every candidate has been
            // compared. If any comparison was UNKNOWN, the answer is UNKNOWN rather than false.
            bool sawUnknown = false;

            foreach (Func<MessageEnvelope, object?> item in items)
            {
                bool? equal = SqlValue.AreEqual(subject, item(message));
                if (equal == true)
                {
                    return !negated;
                }

                if (equal is null)
                {
                    sawUnknown = true;
                }
            }

            return sawUnknown ? null : negated;
        };
    }

    private static Func<MessageEnvelope, object?> CompileIsNull(IsNullExpression expression)
    {
        Func<MessageEnvelope, object?> operand = CompileNode(expression.Operand);
        bool negated = expression.Negated;

        // IS NULL is always definite — it is the one construct that inspects nullness directly
        // rather than comparing against it.
        return message => negated ? operand(message) is not null : operand(message) is null;
    }

    private static Func<MessageEnvelope, object?> CompileExists(ExistsExpression expression)
    {
        string name = expression.Property.Name;

        if (expression.Property.Source == PropertySource.System)
        {
            // System properties always exist; the question is whether one carries a value.
            return message => SystemProperties.Read(message, name) is not null;
        }

        return message => message.ApplicationProperties.ContainsKey(name);
    }
}
