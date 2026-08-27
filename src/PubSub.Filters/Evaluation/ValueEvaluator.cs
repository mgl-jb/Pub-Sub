using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// Builds a delegate that yields an expression's value rather than a match decision.
/// </summary>
/// <remarks>
/// The filter path only ever asks "did this match?", so it collapses UNKNOWN to false at the top.
/// Rule actions need the value itself — <c>SET total = price * quantity</c> assigns a number, not
/// a boolean — so this exposes the underlying evaluation directly.
/// </remarks>
public static class ValueEvaluator
{
    /// <summary>Compiles an expression tree into a value-producing delegate.</summary>
    public static Func<MessageEnvelope, object?> Build(FilterExpression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return FilterCompiler.CompileValue(expression);
    }

    /// <summary>Parses and compiles an expression into a value-producing delegate.</summary>
    public static Func<MessageEnvelope, object?> Build(string expression, FilterLimits? limits = null) =>
        Build(FilterParser.Parse(expression, limits));
}
