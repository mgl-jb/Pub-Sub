using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

public class LexerTests
{
    [Theory]
    [InlineData("a = 1")]
    [InlineData("a=1")]
    [InlineData("  a   =   1  ")]
    [InlineData("a\t=\n1")]
    public void Whitespace_is_insignificant(string expression) =>
        FilterParser.Parse(expression).ToString().ShouldBe("(a = 1)");

    [Theory]
    [InlineData("and")]
    [InlineData("AND")]
    [InlineData("And")]
    public void Keywords_are_case_insensitive(string keyword) =>
        Should.NotThrow(() => FilterParser.Parse($"a = 1 {keyword} b = 2"));

    [Fact]
    public void Property_names_are_case_sensitive()
    {
        // Application properties live in an ordinal dictionary, so the language matches that
        // rather than quietly resolving a differently-cased name.
        MessageEnvelope message = MessageBuilder.Message(new { region = "emea" });

        MessageBuilder.Eval("region = 'emea'", message).ShouldBeTrue();
        MessageBuilder.Eval("Region = 'emea'", message).ShouldBeFalse();
    }

    [Fact]
    public void Doubled_quote_inside_a_string_is_a_literal_quote()
    {
        MessageEnvelope message = MessageBuilder.Message(new { name = "O'Brien" });
        MessageBuilder.Eval("name = 'O''Brien'", message).ShouldBeTrue();
    }

    [Fact]
    public void Unterminated_string_is_rejected() =>
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a = 'unterminated"));

    [Fact]
    public void Bracketed_identifiers_may_contain_spaces()
    {
        MessageEnvelope message = MessageBuilder.Message();
        message.ApplicationProperties["order total"] = 100;

        MessageBuilder.Eval("[order total] = 100", message).ShouldBeTrue();
    }

    [Fact]
    public void Unterminated_bracketed_identifier_is_rejected() =>
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("[unterminated = 1"));

    [Theory]
    [InlineData("42", 42L)]
    [InlineData("-7", -7L)]
    [InlineData("0", 0L)]
    public void Integer_literals_stay_integral(string literal, long expected)
    {
        MessageEnvelope message = MessageBuilder.Message();
        ValueEvaluator.Build(literal)(message).ShouldBe(expected);
    }

    [Theory]
    [InlineData("1.5")]
    [InlineData("1e3")]
    [InlineData("1.5E-2")]
    public void Fractional_and_exponent_literals_parse_as_double(string literal)
    {
        MessageEnvelope message = MessageBuilder.Message();
        ValueEvaluator.Build(literal)(message).ShouldBeOfType<double>();
    }

    [Fact]
    public void Unexpected_character_is_rejected()
    {
        FilterSyntaxException ex =
            Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a = 1 # b"));
        ex.Message.ShouldContain("#");
    }

    [Fact]
    public void Bang_without_equals_is_rejected() =>
        Should.Throw<FilterSyntaxException>(() => FilterParser.Parse("a ! b"));
}
