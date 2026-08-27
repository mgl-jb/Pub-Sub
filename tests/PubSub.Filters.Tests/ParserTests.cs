using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

public class ParserTests
{
    [Fact]
    public void And_binds_tighter_than_or()
    {
        // a OR (b AND c), not (a OR b) AND c.
        FilterParser.Parse("a = 1 OR b = 2 AND c = 3").ToString()
            .ShouldBe("((a = 1) OR ((b = 2) AND (c = 3)))");
    }

    [Fact]
    public void Parentheses_override_precedence()
    {
        FilterParser.Parse("(a = 1 OR b = 2) AND c = 3").ToString()
            .ShouldBe("(((a = 1) OR (b = 2)) AND (c = 3))");
    }

    [Fact]
    public void Not_binds_tighter_than_and()
    {
        FilterParser.Parse("NOT a = 1 AND b = 2").ToString()
            .ShouldBe("(NOT ((a = 1)) AND (b = 2))");
    }

    [Fact]
    public void Multiplication_binds_tighter_than_addition()
    {
        MessageEnvelope message = MessageBuilder.Message();
        ValueEvaluator.Build("2 + 3 * 4")(message).ShouldBe(14L);
    }

    [Fact]
    public void Comparison_binds_looser_than_arithmetic()
    {
        MessageEnvelope message = MessageBuilder.Message(new { price = 10, quantity = 3 });
        MessageBuilder.Eval("price * quantity > 25", message).ShouldBeTrue();
    }

    [Theory]
    [InlineData("<>")]
    [InlineData("!=")]
    public void Both_inequality_spellings_are_accepted(string op)
    {
        MessageEnvelope message = MessageBuilder.Message(new { region = "emea" });
        MessageBuilder.Eval($"region {op} 'apac'", message).ShouldBeTrue();
    }

    [Fact]
    public void Sys_prefix_reads_system_properties()
    {
        MessageEnvelope message = MessageBuilder.Message(subject: "OrderPlaced");
        MessageBuilder.Eval("sys.Subject = 'OrderPlaced'", message).ShouldBeTrue();
    }

    [Fact]
    public void Unknown_system_property_is_rejected_at_parse_time()
    {
        // Catching the typo when the rule is created beats a subscription that silently receives
        // nothing in production.
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("sys.Nonsense = 'x'"));

        ex.Message.ShouldContain("not a known system property");
    }

    [Fact]
    public void Unknown_qualifier_is_rejected()
    {
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("app.region = 'emea'"));

        ex.Message.ShouldContain("not a recognised qualifier");
    }

    [Fact]
    public void System_property_aliases_resolve_to_subject()
    {
        MessageEnvelope message = MessageBuilder.Message(subject: "OrderPlaced");
        MessageBuilder.Eval("sys.Label = 'OrderPlaced'", message).ShouldBeTrue();
        MessageBuilder.Eval("sys.MessageType = 'OrderPlaced'", message).ShouldBeTrue();
    }

    [Fact]
    public void Exists_requires_a_property_argument()
    {
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("EXISTS('literal')"));

        ex.Message.ShouldContain("EXISTS takes a property name");
    }

    [Fact]
    public void Empty_in_list_is_rejected() =>
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("region IN ()"));

    [Fact]
    public void Not_must_be_followed_by_like_or_in_at_comparison_level()
    {
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a NOT b"));

        ex.Message.ShouldContain("Expected LIKE or IN");
    }

    [Fact]
    public void Trailing_tokens_are_rejected()
    {
        // Without this check "a = 1 b = 2" would silently parse as just "a = 1".
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a = 1 b = 2"));
    }

    [Fact]
    public void Unbalanced_parentheses_are_rejected() =>
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("(a = 1"));

    [Fact]
    public void TryParse_reports_failure_without_throwing()
    {
        FilterParser.TryParse("a = = 1", out FilterExpression? result, out string? error)
            .ShouldBeFalse();

        result.ShouldBeNull();
        error.ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public void TryParse_succeeds_for_a_valid_expression()
    {
        FilterParser.TryParse("a = 1", out FilterExpression? result, out string? error).ShouldBeTrue();
        result.ShouldNotBeNull();
        error.ShouldBeNull();
    }

    [Fact]
    public void Syntax_errors_carry_a_position()
    {
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a = 1 AND"));

        ex.Position.ShouldNotBeNull();
    }
}
