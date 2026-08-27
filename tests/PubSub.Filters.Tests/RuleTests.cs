using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

public class RuleTests
{
    [Fact]
    public void True_filter_matches_everything() =>
        FilterCompiler.Compile(TrueFilter.Instance)(MessageBuilder.Message()).ShouldBeTrue();

    [Fact]
    public void False_filter_matches_nothing() =>
        FilterCompiler.Compile(FalseFilter.Instance)(MessageBuilder.Message()).ShouldBeFalse();

    [Fact]
    public void Correlation_filter_combines_conditions_with_and()
    {
        CorrelationFilter filter = new()
        {
            Subject = "OrderPlaced",
            CorrelationId = "trace-1",
        };

        Func<MessageEnvelope, bool> predicate = FilterCompiler.Compile(filter);

        predicate(MessageBuilder.Message(subject: "OrderPlaced", correlationId: "trace-1"))
            .ShouldBeTrue();
        predicate(MessageBuilder.Message(subject: "OrderPlaced", correlationId: "trace-2"))
            .ShouldBeFalse();
        predicate(MessageBuilder.Message(subject: "OrderShipped", correlationId: "trace-1"))
            .ShouldBeFalse();
    }

    [Fact]
    public void Empty_correlation_filter_matches_everything() =>
        FilterCompiler.Compile(new CorrelationFilter())(MessageBuilder.Message()).ShouldBeTrue();

    [Fact]
    public void Correlation_filter_matches_application_properties()
    {
        CorrelationFilter filter = new();
        filter.ApplicationProperties["region"] = "emea";

        Func<MessageEnvelope, bool> predicate = FilterCompiler.Compile(filter);

        predicate(MessageBuilder.Message(new { region = "emea" })).ShouldBeTrue();
        predicate(MessageBuilder.Message(new { region = "apac" })).ShouldBeFalse();
        predicate(MessageBuilder.Message()).ShouldBeFalse();
    }

    [Fact]
    public void Correlation_filter_does_not_match_an_absent_property_against_null()
    {
        // Equality, not presence: an absent property is a non-match even when null is expected.
        CorrelationFilter filter = new();
        filter.ApplicationProperties["region"] = null;

        FilterCompiler.Compile(filter)(MessageBuilder.Message()).ShouldBeFalse();
    }

    [Fact]
    public void Rule_set_combines_rules_with_or()
    {
        RuleSet rules = RuleSet.Compile(
        [
            new RuleDescriptor("emea", new SqlFilter("region = 'emea'")),
            new RuleDescriptor("high-value", new SqlFilter("total > 1000")),
        ]);

        rules.Matches(MessageBuilder.Message(new { region = "emea", total = 10 })).ShouldBeTrue();
        rules.Matches(MessageBuilder.Message(new { region = "apac", total = 5000 })).ShouldBeTrue();
        rules.Matches(MessageBuilder.Message(new { region = "apac", total = 10 })).ShouldBeFalse();
    }

    [Fact]
    public void An_empty_rule_set_matches_nothing()
    {
        // Delivering everything would silently hide what is almost always a misconfiguration.
        RuleSet rules = new([]);

        rules.IsEmpty.ShouldBeTrue();
        rules.Matches(MessageBuilder.Message()).ShouldBeFalse();
    }

    [Fact]
    public void Try_match_reports_the_first_matching_rule()
    {
        RuleSet rules = RuleSet.Compile(
        [
            new RuleDescriptor("first", new SqlFilter("total > 100")),
            new RuleDescriptor("second", new SqlFilter("total > 10")),
        ]);

        rules.TryMatch(MessageBuilder.Message(new { total = 500 }), out CompiledRule? matched)
            .ShouldBeTrue();

        matched!.Name.ShouldBe("first");
    }

    [Fact]
    public void Rule_action_sets_an_application_property()
    {
        CompiledRule rule = CompiledRule.Compile(
            "priority",
            new SqlFilter("total > 1000"),
            new RuleAction("SET priority = 'high'"));

        MessageEnvelope message = MessageBuilder.Message(new { total = 5000 });

        rule.Matches(message).ShouldBeTrue();
        rule.ApplyAction(message);

        message.ApplicationProperties["priority"].ShouldBe("high");
    }

    [Fact]
    public void Rule_action_can_compute_from_existing_properties()
    {
        CompiledRule rule = CompiledRule.Compile(
            "total",
            TrueFilter.Instance,
            new RuleAction("SET lineTotal = price * quantity"));

        MessageEnvelope message = MessageBuilder.Message(new { price = 25, quantity = 4 });
        rule.ApplyAction(message);

        message.ApplicationProperties["lineTotal"].ShouldBe(100L);
    }

    [Fact]
    public void Rule_action_can_remove_a_property()
    {
        CompiledRule rule = CompiledRule.Compile(
            "strip",
            TrueFilter.Instance,
            new RuleAction("REMOVE internalTag"));

        MessageEnvelope message = MessageBuilder.Message(new { internalTag = "secret", keep = 1 });
        rule.ApplyAction(message);

        message.ApplicationProperties.ContainsKey("internalTag").ShouldBeFalse();
        message.ApplicationProperties.ContainsKey("keep").ShouldBeTrue();
    }

    [Fact]
    public void Rule_action_supports_several_clauses()
    {
        CompiledRule rule = CompiledRule.Compile(
            "multi",
            TrueFilter.Instance,
            new RuleAction("SET a = 1; SET b = 'two'; REMOVE c"));

        MessageEnvelope message = MessageBuilder.Message(new { c = "gone" });
        rule.ApplyAction(message);

        message.ApplicationProperties["a"].ShouldBe(1L);
        message.ApplicationProperties["b"].ShouldBe("two");
        message.ApplicationProperties.ContainsKey("c").ShouldBeFalse();
    }

    [Fact]
    public void Rule_action_cannot_modify_a_system_property()
    {
        // System properties are the broker's own routing and delivery state; letting a rule
        // rewrite them would let a subscription corrupt what other subscriptions see.
        FilterSyntaxException ex = Should.Throw<FilterSyntaxException>(
            () => RuleActionCompiler.Compile("SET sys.Subject = 'spoofed'"));

        ex.Message.ShouldContain("cannot modify a system property");
    }

    [Fact]
    public void Rule_action_rejects_an_invalid_property_name() =>
        Should.Throw<FilterSyntaxException>(() => RuleActionCompiler.Compile("SET 9bad = 1"));

    [Fact]
    public void Rule_action_rejects_an_unknown_verb() =>
        Should.Throw<FilterSyntaxException>(() => RuleActionCompiler.Compile("DELETE region"));

    [Fact]
    public void Rule_action_rejects_a_missing_assignment() =>
        Should.Throw<FilterSyntaxException>(() => RuleActionCompiler.Compile("SET region"));

    [Fact]
    public void Rule_action_assignment_tolerates_comparison_operators_in_the_value()
    {
        // The '=' that splits the assignment must be the top-level one, not the '>=' inside the
        // expression on the right.
        CompiledRule rule = CompiledRule.Compile(
            "flag",
            TrueFilter.Instance,
            new RuleAction("SET isLarge = total >= 100"));

        MessageEnvelope message = MessageBuilder.Message(new { total = 250 });
        rule.ApplyAction(message);

        message.ApplicationProperties["isLarge"].ShouldBe(true);
    }

    [Fact]
    public void Rule_action_assignment_ignores_an_equals_inside_a_string()
    {
        CompiledRule rule = CompiledRule.Compile(
            "note",
            TrueFilter.Instance,
            new RuleAction("SET note = 'a=b'"));

        MessageEnvelope message = MessageBuilder.Message();
        rule.ApplyAction(message);

        message.ApplicationProperties["note"].ShouldBe("a=b");
    }

    [Fact]
    public void A_rule_without_an_action_applies_nothing()
    {
        CompiledRule rule = CompiledRule.Compile("plain", TrueFilter.Instance);
        MessageEnvelope message = MessageBuilder.Message(new { a = 1 });

        rule.HasAction.ShouldBeFalse();
        Should.NotThrow(() => rule.ApplyAction(message));
        message.ApplicationProperties.Count.ShouldBe(1);
    }
}
