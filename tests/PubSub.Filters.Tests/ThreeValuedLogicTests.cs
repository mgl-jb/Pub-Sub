using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

/// <summary>
/// The filter language follows SQL's three-valued logic, not C#'s two-valued booleans. These tests
/// pin that down, because the difference is where filter bugs actually come from: a rule that
/// looks like it should match a message with a missing property, and silently does not.
/// </summary>
public class ThreeValuedLogicTests
{
    private static readonly MessageEnvelope Empty = MessageBuilder.Message();

    [Theory]
    [InlineData("missing = 'x'")]
    [InlineData("missing <> 'x'")]
    [InlineData("missing > 1")]
    [InlineData("missing < 1")]
    [InlineData("missing >= 1")]
    [InlineData("missing <= 1")]
    public void Comparing_an_absent_property_yields_unknown(string expression) =>
        MessageBuilder.EvalRaw(expression, Empty).ShouldBeNull();

    [Fact]
    public void Comparing_against_null_yields_unknown_even_for_a_present_property()
    {
        // The classic SQL trap: '= NULL' is never true, not even when the value really is null.
        MessageEnvelope message = MessageBuilder.Message(new { value = (string?)null });

        MessageBuilder.EvalRaw("value = NULL", message).ShouldBeNull();
        MessageBuilder.EvalRaw("value <> NULL", message).ShouldBeNull();
    }

    [Fact]
    public void Is_null_is_the_only_definite_test_for_nullness()
    {
        MessageEnvelope message = MessageBuilder.Message(new { value = (string?)null });

        MessageBuilder.EvalRaw("value IS NULL", message).ShouldBe(true);
        MessageBuilder.EvalRaw("value IS NOT NULL", message).ShouldBe(false);
    }

    [Fact]
    public void Not_unknown_stays_unknown() =>
        MessageBuilder.EvalRaw("NOT (missing = 'x')", Empty).ShouldBeNull();

    [Theory]
    // UNKNOWN AND FALSE is FALSE: no value of the unknown side could make it true.
    [InlineData("missing = 'x' AND 1 = 2", false)]
    // UNKNOWN AND TRUE stays UNKNOWN: the result depends on the unknown side.
    [InlineData("missing = 'x' AND 1 = 1", null)]
    // UNKNOWN OR TRUE is TRUE, by the mirror argument.
    [InlineData("missing = 'x' OR 1 = 1", true)]
    // UNKNOWN OR FALSE stays UNKNOWN.
    [InlineData("missing = 'x' OR 1 = 2", null)]
    public void Unknown_propagates_through_and_or_per_sql(string expression, bool? expected) =>
        MessageBuilder.EvalRaw(expression, Empty).ShouldBe(expected);

    [Fact]
    public void And_or_are_commutative_over_unknown()
    {
        // The short-circuit in the compiler must not change the answer depending on operand order.
        MessageBuilder.EvalRaw("1 = 2 AND missing = 'x'", Empty).ShouldBe(false);
        MessageBuilder.EvalRaw("1 = 1 OR missing = 'x'", Empty).ShouldBe(true);
    }

    [Fact]
    public void Only_true_routes_a_message()
    {
        // The distinction that matters operationally: UNKNOWN does not deliver.
        MessageBuilder.EvalRaw("missing = 'x'", Empty).ShouldBeNull();
        MessageBuilder.Eval("missing = 'x'", Empty).ShouldBeFalse();
    }

    [Fact]
    public void Comparing_mismatched_types_yields_unknown_rather_than_throwing()
    {
        // A filter runs against every message on the topic, including ones whose shape its author
        // never saw. Throwing would turn one odd message into a failure for everyone.
        MessageEnvelope message = MessageBuilder.Message(new { value = "not-a-number" });

        MessageBuilder.EvalRaw("value > 5", message).ShouldBeNull();
        Should.NotThrow(() => MessageBuilder.Eval("value > 5", message));
    }

    [Fact]
    public void Exists_distinguishes_absent_from_null()
    {
        // EXISTS asks about presence; IS NOT NULL asks about the value. A property explicitly set
        // to null is present but null, and only EXISTS can tell you so.
        MessageEnvelope present = MessageBuilder.Message(new { value = (string?)null });
        MessageEnvelope absent = MessageBuilder.Message();

        MessageBuilder.Eval("EXISTS(value)", present).ShouldBeTrue();
        MessageBuilder.Eval("value IS NOT NULL", present).ShouldBeFalse();
        MessageBuilder.Eval("EXISTS(value)", absent).ShouldBeFalse();
    }

    [Fact]
    public void In_yields_unknown_when_no_match_and_a_comparison_was_unknown()
    {
        MessageEnvelope message = MessageBuilder.Message(new { region = "apac" });

        // 'apac' matches neither, but comparing it to NULL is UNKNOWN, so the whole test is.
        MessageBuilder.EvalRaw("region IN ('emea', NULL)", message).ShouldBeNull();

        // With no NULL in the list the non-match is definite.
        MessageBuilder.EvalRaw("region IN ('emea', 'amer')", message).ShouldBe(false);
    }

    [Fact]
    public void In_short_circuits_on_a_definite_match()
    {
        MessageEnvelope message = MessageBuilder.Message(new { region = "emea" });
        MessageBuilder.EvalRaw("region IN ('emea', NULL)", message).ShouldBe(true);
    }

    [Fact]
    public void Like_against_a_non_string_yields_unknown()
    {
        MessageEnvelope message = MessageBuilder.Message(new { total = 100 });
        MessageBuilder.EvalRaw("total LIKE '1%'", message).ShouldBeNull();
    }

    [Fact]
    public void Arithmetic_on_an_absent_property_yields_unknown() =>
        MessageBuilder.EvalRaw("missing + 1 = 2", Empty).ShouldBeNull();

    [Fact]
    public void Division_by_zero_yields_unknown_rather_than_throwing()
    {
        MessageEnvelope message = MessageBuilder.Message(new { total = 10, divisor = 0 });

        Should.NotThrow(() => MessageBuilder.Eval("total / divisor > 1", message));
        MessageBuilder.EvalRaw("total / divisor > 1", message).ShouldBeNull();
    }
}
