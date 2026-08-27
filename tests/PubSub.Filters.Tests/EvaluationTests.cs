using System.Globalization;
using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

public class EvaluationTests
{
    [Fact]
    public void Numeric_comparison_works_across_clr_numeric_types()
    {
        // Application properties arrive from JSON, so the same logical number can land as int,
        // long, double, or decimal. A filter should not care which.
        foreach (object value in new object[] { 100, 100L, 100.0, 100m, (short)100, (byte)100 })
        {
            MessageEnvelope message = MessageBuilder.Message();
            message.ApplicationProperties["total"] = value;

            MessageBuilder.Eval("total = 100", message)
                .ShouldBeTrue($"a {value.GetType().Name} of 100 should equal the literal 100");
            MessageBuilder.Eval("total > 99", message).ShouldBeTrue();
            MessageBuilder.Eval("total < 101", message).ShouldBeTrue();
        }
    }

    [Fact]
    public void String_comparison_is_ordinal()
    {
        MessageEnvelope message = MessageBuilder.Message(new { region = "EMEA" });

        MessageBuilder.Eval("region = 'EMEA'", message).ShouldBeTrue();
        MessageBuilder.Eval("region = 'emea'", message).ShouldBeFalse();
    }

    [Fact]
    public void Boolean_properties_compare_by_equality()
    {
        MessageEnvelope message = MessageBuilder.Message(new { urgent = true });

        MessageBuilder.Eval("urgent = TRUE", message).ShouldBeTrue();
        MessageBuilder.Eval("urgent = FALSE", message).ShouldBeFalse();
    }

    [Fact]
    public void Integer_arithmetic_stays_integral()
    {
        // The result must not silently widen to decimal or double; a caller assigning it through a
        // rule action would otherwise see 3 become 3.0.
        MessageEnvelope message = MessageBuilder.Message();
        ValueEvaluator.Build("1 + 2")(message).ShouldBeOfType<long>().ShouldBe(3L);
        ValueEvaluator.Build("10 - 4")(message).ShouldBeOfType<long>().ShouldBe(6L);
        ValueEvaluator.Build("3 * 4")(message).ShouldBeOfType<long>().ShouldBe(12L);
        ValueEvaluator.Build("10 % 3")(message).ShouldBeOfType<long>().ShouldBe(1L);
    }

    [Fact]
    public void Division_produces_a_fraction_even_from_integers()
    {
        MessageEnvelope message = MessageBuilder.Message();
        Convert.ToDouble(ValueEvaluator.Build("7 / 2")(message), CultureInfo.InvariantCulture)
            .ShouldBe(3.5, 1e-9);
    }

    [Fact]
    public void Unary_minus_negates()
    {
        MessageEnvelope message = MessageBuilder.Message(new { balance = 50 });
        MessageBuilder.Eval("-balance = -50", message).ShouldBeTrue();
    }

    [Fact]
    public void String_concatenation_uses_plus()
    {
        MessageEnvelope message = MessageBuilder.Message(new { first = "order", second = "-123" });
        ValueEvaluator.Build("first + second")(message).ShouldBe("order-123");
    }

    [Theory]
    [InlineData("order-2026-001", "order-%", true)]
    [InlineData("order-2026-001", "%-001", true)]
    [InlineData("order-2026-001", "order-____-001", true)]
    [InlineData("order-2026-001", "order-___-001", false)]
    [InlineData("order-2026-001", "invoice-%", false)]
    [InlineData("abc", "abc", true)]
    public void Like_matches_sql_wildcards(string value, string pattern, bool expected)
    {
        MessageEnvelope message = MessageBuilder.Message(new { reference = value });
        MessageBuilder.Eval($"reference LIKE '{pattern}'", message).ShouldBe(expected);
    }

    [Fact]
    public void Like_is_anchored_to_the_whole_value()
    {
        // Without anchoring, 'order' would match 'preorder-1' — a routing bug that is easy to ship.
        MessageEnvelope message = MessageBuilder.Message(new { reference = "preorder-1" });
        MessageBuilder.Eval("reference LIKE 'order'", message).ShouldBeFalse();
    }

    [Fact]
    public void Like_treats_regex_metacharacters_literally()
    {
        // The pattern is SQL LIKE, not a regex: '.' and '*' are ordinary characters.
        MessageEnvelope dotted = MessageBuilder.Message(new { reference = "a.c" });
        MessageEnvelope other = MessageBuilder.Message(new { reference = "abc" });

        MessageBuilder.Eval("reference LIKE 'a.c'", dotted).ShouldBeTrue();
        MessageBuilder.Eval("reference LIKE 'a.c'", other).ShouldBeFalse();
    }

    [Fact]
    public void Like_escape_makes_a_wildcard_literal()
    {
        MessageEnvelope literal = MessageBuilder.Message(new { reference = "100%" });
        MessageEnvelope other = MessageBuilder.Message(new { reference = "100 percent" });

        MessageBuilder.Eval(@"reference LIKE '100!%' ESCAPE '!'", literal).ShouldBeTrue();
        MessageBuilder.Eval(@"reference LIKE '100!%' ESCAPE '!'", other).ShouldBeFalse();
    }

    [Fact]
    public void Not_like_negates()
    {
        MessageEnvelope message = MessageBuilder.Message(new { reference = "invoice-1" });
        MessageBuilder.Eval("reference NOT LIKE 'order-%'", message).ShouldBeTrue();
    }

    [Fact]
    public void In_and_not_in_test_membership()
    {
        MessageEnvelope message = MessageBuilder.Message(new { region = "emea" });

        MessageBuilder.Eval("region IN ('emea', 'apac')", message).ShouldBeTrue();
        MessageBuilder.Eval("region IN ('amer')", message).ShouldBeFalse();
        MessageBuilder.Eval("region NOT IN ('amer')", message).ShouldBeTrue();
    }

    [Fact]
    public void In_accepts_expressions_not_only_literals()
    {
        MessageEnvelope message = MessageBuilder.Message(new { total = 20, baseValue = 10 });
        MessageBuilder.Eval("total IN (baseValue * 2, 99)", message).ShouldBeTrue();
    }

    [Fact]
    public void A_realistic_composite_rule_routes_correctly()
    {
        MessageEnvelope matching = MessageBuilder.Message(
            new { region = "emea", total = 750, channel = "web" },
            subject: "OrderPlaced");

        MessageEnvelope wrongRegion = MessageBuilder.Message(
            new { region = "apac", total = 750, channel = "web" },
            subject: "OrderPlaced");

        MessageEnvelope belowThreshold = MessageBuilder.Message(
            new { region = "emea", total = 100, channel = "web" },
            subject: "OrderPlaced");

        const string Rule =
            "sys.Subject = 'OrderPlaced' AND region IN ('emea', 'amer') " +
            "AND total > 500 AND channel <> 'internal'";

        MessageBuilder.Eval(Rule, matching).ShouldBeTrue();
        MessageBuilder.Eval(Rule, wrongRegion).ShouldBeFalse();
        MessageBuilder.Eval(Rule, belowThreshold).ShouldBeFalse();
    }

    [Fact]
    public void System_numeric_properties_are_comparable()
    {
        MessageEnvelope message = MessageBuilder.Message(sequenceNumber: 42, deliveryCount: 3);

        MessageBuilder.Eval("sys.SequenceNumber > 40", message).ShouldBeTrue();
        MessageBuilder.Eval("sys.DeliveryCount >= 3", message).ShouldBeTrue();
    }

    [Fact]
    public void Compiled_filters_are_reusable_across_messages()
    {
        Func<MessageEnvelope, bool> predicate = FilterCompiler.Compile("total > 100");

        predicate(MessageBuilder.Message(new { total = 150 })).ShouldBeTrue();
        predicate(MessageBuilder.Message(new { total = 50 })).ShouldBeFalse();
        predicate(MessageBuilder.Message()).ShouldBeFalse();
    }
}
