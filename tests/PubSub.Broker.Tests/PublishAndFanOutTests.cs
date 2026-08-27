using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

[Collection(BrokerCollection.Name)]
public class PublishAndFanOutTests
{
    private readonly BrokerFixture _fixture;

    public PublishAndFanOutTests(BrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task A_published_message_reaches_its_subscription()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        await _fixture.PublishAsync(topic, TestHelpers.Message("hello"));

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        received.Count.ShouldBe(1);
        received[0].Message.BodyText().ShouldBe("hello");
    }

    [Fact]
    public async Task Fan_out_reaches_exactly_the_subscriptions_whose_rules_match()
    {
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);

            await admin.CreateSubscriptionAsync(topic, "all", rule:
                new RuleDescriptor("all", TrueFilter.Instance));

            await admin.CreateSubscriptionAsync(topic, "high-value", rule:
                new RuleDescriptor("high", new SqlFilter("total > 500")));

            await admin.CreateSubscriptionAsync(topic, "emea", rule:
                new RuleDescriptor("emea", new SqlFilter("region = 'emea'")));
        });

        await _fixture.PublishAsync(topic, TestHelpers.Message(
            "order",
            properties: new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["total"] = 1000,
                ["region"] = "apac",
            }));

        (await _fixture.ReceiveAsync(topic, "all")).Count.ShouldBe(1);
        (await _fixture.ReceiveAsync(topic, "high-value")).Count.ShouldBe(1);

        // The region filter does not match, so this subscription must see nothing at all.
        (await _fixture.ReceiveAsync(topic, "emea")).ShouldBeEmpty();
    }

    [Fact]
    public async Task Each_subscription_settles_independently()
    {
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);
            await admin.CreateSubscriptionAsync(topic, "fast");
            await admin.CreateSubscriptionAsync(topic, "slow");
        });

        await _fixture.PublishAsync(topic, TestHelpers.Message("shared"));

        IReadOnlyList<ReceivedMessage> fast = await _fixture.ReceiveAsync(topic, "fast");
        fast.Count.ShouldBe(1);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .CompleteAsync(fast[0].DeliveryId, fast[0].LockToken));

        // One subscription completing the message must not remove it from the other's queue.
        IReadOnlyList<ReceivedMessage> slow = await _fixture.ReceiveAsync(topic, "slow");
        slow.Count.ShouldBe(1);
        slow[0].Message.BodyText().ShouldBe("shared");
    }

    [Fact]
    public async Task The_message_body_is_stored_once_regardless_of_fan_out()
    {
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);
            for (int i = 0; i < 3; i++)
            {
                await admin.CreateSubscriptionAsync(topic, $"sub{i}");
            }
        });

        IReadOnlyList<PublishResult> results =
            await _fixture.PublishAsync(topic, TestHelpers.Message("body"));

        results[0].MatchedSubscriptions.ShouldBe(3);

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerDbContext context = services.GetRequiredService<BrokerDbContext>();

            int messages = await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions
                .CountAsync(context.Messages.Where(m => m.Topic!.Name == topic));

            int deliveries = await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions
                .CountAsync(context.Deliveries.Where(d => d.Subscription!.Topic!.Name == topic));

            messages.ShouldBe(1, "the payload is stored once per topic");
            deliveries.ShouldBe(3, "delivery state is per subscription");
        });
    }

    [Fact]
    public async Task Sequence_numbers_increase_in_publish_order()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        IReadOnlyList<PublishResult> results = await _fixture.PublishAsync(
            topic,
            TestHelpers.Message("first"),
            TestHelpers.Message("second"),
            TestHelpers.Message("third"));

        results[0].SequenceNumber.ShouldBeLessThan(results[1].SequenceNumber);
        results[1].SequenceNumber.ShouldBeLessThan(results[2].SequenceNumber);

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
        received.Select(r => r.Message.BodyText()).ShouldBe(["first", "second", "third"]);
    }

    [Fact]
    public async Task A_rule_action_rewrites_only_its_own_subscriptions_copy()
    {
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);

            await admin.CreateSubscriptionAsync(topic, "tagged", rule: new RuleDescriptor(
                "tag",
                TrueFilter.Instance,
                new RuleAction("SET priority = 'high'")));

            await admin.CreateSubscriptionAsync(topic, "plain");
        });

        await _fixture.PublishAsync(topic, TestHelpers.Message("order"));

        IReadOnlyList<ReceivedMessage> tagged = await _fixture.ReceiveAsync(topic, "tagged");
        IReadOnlyList<ReceivedMessage> plain = await _fixture.ReceiveAsync(topic, "plain");

        tagged[0].Message.ApplicationProperties["priority"].ShouldBe("high");
        plain[0].Message.ApplicationProperties.ContainsKey("priority").ShouldBeFalse();
    }

    [Fact]
    public async Task Publishing_to_an_unknown_topic_is_rejected() =>
        await Should.ThrowAsync<EntityNotFoundException>(
            () => _fixture.PublishAsync("no-such-topic", TestHelpers.Message()));

    [Fact]
    public async Task A_message_matching_no_subscription_is_stored_but_routed_nowhere()
    {
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);
            await admin.CreateSubscriptionAsync(topic, "picky", rule:
                new RuleDescriptor("picky", new SqlFilter("region = 'nowhere'")));
        });

        IReadOnlyList<PublishResult> results =
            await _fixture.PublishAsync(topic, TestHelpers.Message("orphan"));

        results[0].MatchedSubscriptions.ShouldBe(0);
        (await _fixture.ReceiveAsync(topic, "picky")).ShouldBeEmpty();
    }
}
