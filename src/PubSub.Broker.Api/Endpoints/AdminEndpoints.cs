using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Api;

/// <summary>Topic, subscription, and rule management.</summary>
public static class AdminEndpoints
{
    /// <summary>Maps the administration routes.</summary>
    public static IEndpointRouteBuilder MapAdminEndpoints(this IEndpointRouteBuilder routes)
    {
        ArgumentNullException.ThrowIfNull(routes);

        RouteGroupBuilder admin = routes.MapGroup("/topics")
            .WithTags("Administration")
            .RequireAuthorization(BrokerPolicies.Admin);

        admin.MapGet("/", ListTopicsAsync)
            .WithName("ListTopics")
            .WithSummary("Lists every topic.");

        admin.MapPut("/{topic}", CreateTopicAsync)
            .WithName("CreateTopic")
            .WithSummary("Creates a topic, or returns the existing one unchanged.");

        admin.MapDelete("/{topic}", DeleteTopicAsync)
            .WithName("DeleteTopic")
            .WithSummary("Deletes a topic together with its subscriptions and messages.");

        admin.MapGet("/{topic}/subscriptions", ListSubscriptionsAsync)
            .WithName("ListSubscriptions")
            .WithSummary("Lists a topic's subscriptions.");

        admin.MapPut("/{topic}/subscriptions/{subscription}", CreateSubscriptionAsync)
            .WithName("CreateSubscription")
            .WithSummary("Creates a subscription, with a catch-all rule unless one is supplied.");

        admin.MapDelete("/{topic}/subscriptions/{subscription}", DeleteSubscriptionAsync)
            .WithName("DeleteSubscription")
            .WithSummary("Deletes a subscription and everything queued for it.");

        admin.MapGet("/{topic}/subscriptions/{subscription}/rules", ListRulesAsync)
            .WithName("ListRules")
            .WithSummary("Lists a subscription's rules.");

        admin.MapPut("/{topic}/subscriptions/{subscription}/rules/{rule}", AddRuleAsync)
            .WithName("AddRule")
            .WithSummary("Adds a rule, rejecting a malformed filter before it is stored.");

        admin.MapDelete("/{topic}/subscriptions/{subscription}/rules/{rule}", RemoveRuleAsync)
            .WithName("RemoveRule")
            .WithSummary("Removes a rule from a subscription.");

        return routes;
    }

    private static async Task<Ok<IReadOnlyList<TopicDto>>> ListTopicsAsync(
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<TopicEntity> topics = await admin.ListTopicsAsync(cancellationToken);
        return TypedResults.Ok<IReadOnlyList<TopicDto>>([.. topics.Select(t => t.ToDto())]);
    }

    private static async Task<Ok<TopicDto>> CreateTopicAsync(
        string topic,
        [FromBody] CreateTopicDto request,
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        // PUT is idempotent, so creating a topic that already exists succeeds rather than
        // conflicting — which is what makes this safe to run from a deployment pipeline.
        TopicEntity created = await admin.EnsureTopicAsync(topic, request.ToOptions(), cancellationToken);
        return TypedResults.Ok(created.ToDto());
    }

    private static async Task<IResult> DeleteTopicAsync(
        string topic,
        BrokerAdmin admin,
        CancellationToken cancellationToken) =>
        await admin.DeleteTopicAsync(topic, cancellationToken)
            ? TypedResults.NoContent()
            : TypedResults.NotFound();

    private static async Task<Ok<IReadOnlyList<SubscriptionDto>>> ListSubscriptionsAsync(
        string topic,
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<SubscriptionEntity> subscriptions =
            await admin.ListSubscriptionsAsync(topic, cancellationToken);

        return TypedResults.Ok<IReadOnlyList<SubscriptionDto>>(
            [.. subscriptions.Select(s => s.ToDto())]);
    }

    private static async Task<IResult> CreateSubscriptionAsync(
        string topic,
        string subscription,
        [FromBody] CreateSubscriptionDto request,
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        try
        {
            SubscriptionEntity created = await admin.CreateSubscriptionAsync(
                topic,
                subscription,
                request.ToOptions(),
                request.Rule?.ToDescriptor(),
                cancellationToken);

            return TypedResults.Ok(created.ToDto());
        }
        catch (EntityAlreadyExistsException)
        {
            // Creating a subscription that already exists is treated as satisfied, so this stays
            // safe to re-run. Its settings are not silently rewritten, though.
            IReadOnlyList<SubscriptionEntity> existing =
                await admin.ListSubscriptionsAsync(topic, cancellationToken);

            SubscriptionEntity? match = existing.FirstOrDefault(s =>
                string.Equals(s.Name, subscription, StringComparison.Ordinal));

            return match is null ? TypedResults.NotFound() : TypedResults.Ok(match.ToDto());
        }
    }

    private static async Task<IResult> DeleteSubscriptionAsync(
        string topic,
        string subscription,
        BrokerAdmin admin,
        CancellationToken cancellationToken) =>
        await admin.DeleteSubscriptionAsync(topic, subscription, cancellationToken)
            ? TypedResults.NoContent()
            : TypedResults.NotFound();

    private static async Task<Ok<IReadOnlyList<RuleDto>>> ListRulesAsync(
        string topic,
        string subscription,
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RuleEntity> rules =
            await admin.ListRulesAsync(topic, subscription, cancellationToken);

        return TypedResults.Ok<IReadOnlyList<RuleDto>>([.. rules.Select(r => r.ToDto())]);
    }

    private static async Task<Ok<RuleDto>> AddRuleAsync(
        string topic,
        string subscription,
        string rule,
        [FromBody] CreateRuleDto request,
        BrokerAdmin admin,
        CancellationToken cancellationToken)
    {
        // The route segment is authoritative, so a mismatched body cannot create a differently
        // named rule than the URL claims.
        CreateRuleDto named = request with { Name = rule };

        RuleEntity created =
            await admin.AddRuleAsync(topic, subscription, named.ToDescriptor(), cancellationToken);

        return TypedResults.Ok(created.ToDto());
    }

    private static async Task<IResult> RemoveRuleAsync(
        string topic,
        string subscription,
        string rule,
        BrokerAdmin admin,
        CancellationToken cancellationToken) =>
        await admin.RemoveRuleAsync(topic, subscription, rule, cancellationToken)
            ? TypedResults.NoContent()
            : TypedResults.NotFound();
}
