using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

[Collection(BrokerCollection.Name)]
public class SchedulingAndExpiryTests
{
    private readonly BrokerFixture _fixture;

    public SchedulingAndExpiryTests(BrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task A_scheduled_message_is_invisible_until_its_time()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        DateTimeOffset dueAt = _fixture.Clock.GetUtcNow().AddMinutes(30);
        await _fixture.PublishAsync(topic, TestHelpers.Message("later", scheduledFor: dueAt));

        (await _fixture.ReceiveAsync(topic, subscription)).ShouldBeEmpty();

        _fixture.Clock.Advance(TimeSpan.FromMinutes(29));
        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("still a minute early");

        _fixture.Clock.Advance(TimeSpan.FromMinutes(2));
        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        received.Count.ShouldBe(1);
        received[0].Message.BodyText().ShouldBe("later");
    }

    [Fact]
    public async Task A_scheduled_messages_lifetime_starts_when_it_becomes_visible()
    {
        // Measuring time to live from publish would expire a message scheduled beyond its own TTL
        // before it could ever be delivered.
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        DateTimeOffset dueAt = _fixture.Clock.GetUtcNow().AddHours(2);

        await _fixture.PublishAsync(topic, TestHelpers.Message(
            "delayed",
            scheduledFor: dueAt,
            timeToLive: TimeSpan.FromMinutes(30)));

        _fixture.Clock.Advance(TimeSpan.FromHours(2).Add(TimeSpan.FromMinutes(1)));
        await _fixture.SweepAsync();

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        received.Count.ShouldBe(
            1,
            "a 30-minute lifetime beginning two hours from now has not lapsed one minute in");
    }

    [Fact]
    public async Task An_expired_message_is_dead_lettered_by_default()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { DeadLetterOnMessageExpiration = true });

        await _fixture.PublishAsync(topic, TestHelpers.Message(
            "perishable",
            timeToLive: TimeSpan.FromMinutes(10)));

        _fixture.Clock.Advance(TimeSpan.FromMinutes(11));
        await _fixture.SweepAsync();

        (await _fixture.ReceiveAsync(topic, subscription)).ShouldBeEmpty();

        IReadOnlyList<ReceivedMessage> deadLettered =
            await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true);

        deadLettered.Count.ShouldBe(
            1,
            "a message vanishing at expiry is indistinguishable from one that was lost");
        deadLettered[0].Message.DeadLetterReason.ShouldBe(DeadLetterReason.TimeToLiveExpired);
    }

    [Fact]
    public async Task An_expired_message_is_discarded_when_the_subscription_says_so()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { DeadLetterOnMessageExpiration = false });

        await _fixture.PublishAsync(topic, TestHelpers.Message(timeToLive: TimeSpan.FromMinutes(5)));

        _fixture.Clock.Advance(TimeSpan.FromMinutes(6));
        await _fixture.SweepAsync();

        (await _fixture.ReceiveAsync(topic, subscription)).ShouldBeEmpty();
        (await _fixture.ReceiveAsync(topic, subscription, fromDeadLetterQueue: true)).ShouldBeEmpty();
    }

    [Fact]
    public async Task Deferring_removes_a_message_from_the_normal_flow()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message("out-of-turn"));

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
        long sequenceNumber = received[0].Message.SequenceNumber;

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .DeferAsync(received[0].DeliveryId, received[0].LockToken));

        _fixture.Clock.Advance(TimeSpan.FromMinutes(5));
        await _fixture.SweepAsync();

        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("a deferred message is only reachable by sequence number");

        IReadOnlyList<ReceivedMessage> deferred = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReceiveDeferredAsync(topic, subscription, [sequenceNumber]));

        deferred.Count.ShouldBe(1);
        deferred[0].Message.BodyText().ShouldBe("out-of-turn");
    }

    [Fact]
    public async Task A_deferred_message_can_be_completed_after_retrieval()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message());

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);
        long sequenceNumber = received[0].Message.SequenceNumber;

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .DeferAsync(received[0].DeliveryId, received[0].LockToken));

        IReadOnlyList<ReceivedMessage> deferred = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReceiveDeferredAsync(topic, subscription, [sequenceNumber]));

        SettlementResult result = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .CompleteAsync(deferred[0].DeliveryId, deferred[0].LockToken));

        result.ShouldBe(SettlementResult.Settled);
    }

    [Fact]
    public async Task Requesting_an_unknown_deferred_sequence_number_returns_nothing()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        IReadOnlyList<ReceivedMessage> deferred = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReceiveDeferredAsync(topic, subscription, [999_999]));

        deferred.ShouldBeEmpty();
    }
}
