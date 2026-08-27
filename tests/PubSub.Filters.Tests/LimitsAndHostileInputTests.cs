using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

/// <summary>
/// Filter expressions are attacker-influenced input wherever subscriptions can be created by
/// someone other than the operator, and every rule is evaluated against every message on its
/// topic. These tests pin down the bounds that keep one bad expression from becoming everyone's
/// problem.
/// </summary>
public class LimitsAndHostileInputTests
{
    [Fact]
    public void Expression_length_is_capped()
    {
        string huge = "a = 1 AND " + string.Join(" AND ", Enumerable.Repeat("b = 2", 2000));

        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse(huge));

        ex.Message.ShouldContain("exceeds the maximum");
    }

    [Fact]
    public void Deep_nesting_is_rejected_rather_than_overflowing_the_stack()
    {
        // A recursive-descent parser hits a StackOverflowException on deeply nested input, and
        // that kills the process outright — it cannot be caught. The depth cap turns it into an
        // ordinary rejected expression.
        const int Depth = 500;
        string nested = new string('(', Depth) + "a = 1" + new string(')', Depth);

        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse(nested));

        ex.Message.ShouldContain("nests deeper");
    }

    [Fact]
    public void In_list_size_is_capped()
    {
        string list = string.Join(", ", Enumerable.Range(0, 500).Select(i => $"'{i}'"));

        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse($"region IN ({list})"));

        ex.Message.ShouldContain("at most");
    }

    [Fact]
    public void Identifier_length_is_capped()
    {
        string name = new('a', 500);
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse($"{name} = 1"));
    }

    [Fact]
    public void String_literal_length_is_capped()
    {
        string literal = new('x', 5000);
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse($"a = '{literal}'"));
    }

    [Fact]
    public void Limits_are_configurable()
    {
        FilterLimits strict = new() { MaxDepth = 2 };

        Should.Throw<FilterSyntaxException>(
            () => FilterParser.Parse("((((a = 1))))", strict));

        Should.NotThrow(() => FilterParser.Parse("((((a = 1))))"));
    }

    [Theory]
    // A filter is parsed by us and never concatenated into database SQL, so these are just
    // ordinary strings or syntax errors — never anything the database sees.
    [InlineData("region = 'emea'; DROP TABLE Messages--'")]
    [InlineData("region = 'x'' OR ''1''=''1'")]
    [InlineData("region = 'x' UNION SELECT * FROM Deliveries")]
    public void Sql_injection_shaped_input_is_inert(string expression)
    {
        // Either it parses as a plain string comparison, or it is rejected as malformed. What it
        // must never do is reach a database.
        try
        {
            Func<MessageEnvelope, bool> predicate = FilterCompiler.Compile(expression);
            predicate(MessageBuilder.Message(new { region = "emea" }));
        }
        catch (FilterSyntaxException)
        {
            // A rejected expression is an equally acceptable outcome.
        }
    }

    [Fact]
    public void A_catastrophically_backtracking_pattern_cannot_be_smuggled_through_like()
    {
        // LIKE patterns are escaped before translation, so '(a+)+$' matches those literal
        // characters rather than becoming a regex bomb.
        MessageEnvelope message = MessageBuilder.Message(
            new { reference = new string('a', 40) + "!" });

        System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
        MessageBuilder.Eval("reference LIKE '(a+)+$'", message).ShouldBeFalse();
        stopwatch.Stop();

        stopwatch.ElapsedMilliseconds.ShouldBeLessThan(1000);
    }

    [Fact]
    public void Overflowing_arithmetic_yields_unknown_rather_than_throwing()
    {
        MessageEnvelope message = MessageBuilder.Message();
        message.ApplicationProperties["big"] = decimal.MaxValue;

        Should.NotThrow(() => MessageBuilder.Eval("big * big > 0", message));
        MessageBuilder.EvalRaw("big * big > 0", message).ShouldBeNull();
    }

    [Fact]
    public void Non_finite_doubles_are_treated_as_incomparable()
    {
        MessageEnvelope message = MessageBuilder.Message();
        message.ApplicationProperties["value"] = double.NaN;

        MessageBuilder.EvalRaw("value > 0", message).ShouldBeNull();
    }

    [Fact]
    public void Empty_and_whitespace_expressions_are_rejected()
    {
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse(string.Empty));
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("   "));
    }
}
