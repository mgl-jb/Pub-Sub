using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

[Collection(BrokerCollection.Name)]
public class DeadLetterTests
{
    private readonly BrokerFixture _fixture;

    public DeadLetterTests(BrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Exceeding_the_delivery_budget_dead_letters_the_message()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { MaxDeliveryCount = 3 });

        await _fixture.PublishAsync(topic, TestHelpers.Message("poison"));

        for (int attempt = 1; attempt <= 3; attempt++)
        {
            IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
            received.Count.ShouldBe(1, $"attempt {attempt} should still deliver the message");

            await _fixture.WithScopeAsync(services =>
                services.GetRequiredService<BrokerStore>()
                    .AbandonAsync(received[0].DeliveryId, received[0].LockToken));
        }

        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("the budget is spent; the message must stop consuming consumer time");

        IReadOnlyList<ReceivedMessage> deadLettered =
            await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true);

        deadLettered.Count.ShouldBe(1);
        deadLettered[0].Message.DeadLetterReason.ShouldBe(DeadLetterReason.MaxDeliveryCountExceeded);
        deadLettered[0].Message.BodyText().ShouldBe("poison");
    }

    [Fact]
    public async Task Dead_lettering_explicitly_skips_the_remaining_retries()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { MaxDeliveryCount = 10 });

        await _fixture.PublishAsync(topic, TestHelpers.Message("malformed"));
        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().DeadLetterAsync(
                received[0].DeliveryId,
                received[0].LockToken,
                DeadLetterReason.DeserializationError,
                "The payload is not valid JSON."));

        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("retrying a message that can never succeed only wastes attempts");

        IReadOnlyList<ReceivedMessage> deadLettered =
            await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true);

        deadLettered[0].Message.DeadLetterReason.ShouldBe(DeadLetterReason.DeserializationError);
        deadLettered[0].Message.DeadLetterDescription.ShouldBe("The payload is not valid JSON.");
    }

    [Fact]
    public async Task Replaying_returns_dead_lettered_messages_with_a_fresh_budget()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { MaxDeliveryCount = 2 });

        await _fixture.PublishAsync(topic, TestHelpers.Message("recoverable"));

        for (int i = 0; i < 2; i++)
        {
            IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
            await _fixture.WithScopeAsync(services =>
                services.GetRequiredService<BrokerStore>()
                    .AbandonAsync(received[0].DeliveryId, received[0].LockToken));
        }

        int replayed = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReplayDeadLetteredAsync(topic, subscription));

        replayed.ShouldBe(1);

        IReadOnlyList<ReceivedMessage> redelivered = await _fixture.ReceiveAsync(topic, subscription);

        redelivered.Count.ShouldBe(1);
        redelivered[0].Message.BodyText().ShouldBe("recoverable");
        redelivered[0].Message.DeliveryCount.ShouldBe(
            1,
            "a replay follows a fix, so charging the message for past failures would dead-letter " +
            "it again immediately");
    }

    [Fact]
    public async Task Reading_the_dead_letter_queue_does_not_consume_the_retry_budget()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { MaxDeliveryCount = 1 });

        await _fixture.PublishAsync(topic, TestHelpers.Message());
        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AbandonAsync(received[0].DeliveryId, received[0].LockToken));

        IReadOnlyList<ReceivedMessage> firstPeek =
            await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true);
        firstPeek.Count.ShouldBe(1);

        int countAfterPeek = firstPeek[0].Message.DeliveryCount;

        _fixture.Clock.Advance(TimeSpan.FromMinutes(5));
        await _fixture.SweepAsync();

        IReadOnlyList<ReceivedMessage> secondPeek =
            await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true);

        secondPeek.Count.ShouldBe(1, "the message stays in the dead-letter queue until replayed");
        secondPeek[0].Message.DeliveryCount.ShouldBe(
            countAfterPeek,
            "browsing the dead-letter queue must not look like another delivery attempt");
        secondPeek[0].Message.DeadLetterReason.ShouldNotBeNull(
            "the original reason must survive a browse");
    }

    [Fact]
    public async Task A_rule_that_throws_dead_letters_rather_than_dropping_the_message()
    {
        // Dropping silently would be indistinguishable from a routing gap, which is far harder to
        // diagnose than a message sitting in the dead-letter queue with a reason attached.
        string topic = BrokerFixture.UniqueName("t");

        await _fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic);
            await admin.CreateSubscriptionAsync(
                topic,
                "guarded",
                new SubscriptionOptions { DeadLetterOnFilterEvaluationError = true });
        });

        await _fixture.PublishAsync(topic, TestHelpers.Message("ok"));

        (await _fixture.ReceiveAsync(topic, "guarded")).Count.ShouldBe(
            1,
            "a well-formed rule routes normally");
    }
}
