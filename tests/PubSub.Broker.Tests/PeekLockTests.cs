using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

/// <summary>
/// The peek-lock contract: exactly one receiver holds a message at a time, settlement requires a
/// live lock, and a lost lock returns the message rather than losing it.
/// </summary>
[Collection(BrokerCollection.Name)]
public class PeekLockTests
{
    private readonly BrokerFixture _fixture;

    public PeekLockTests(BrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task A_locked_message_is_invisible_to_other_receivers()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message("only-one"));

        IReadOnlyList<ReceivedMessage> first = await _fixture.ReceiveAsync(topic, subscription);
        IReadOnlyList<ReceivedMessage> second = await _fixture.ReceiveAsync(topic, subscription);

        first.Count.ShouldBe(1);
        second.ShouldBeEmpty("the message is locked by the first receiver");
    }

    [Fact]
    public async Task Competing_consumers_receive_disjoint_message_sets()
    {
        // This is the property READPAST exists for: without it the receivers below would block on
        // each other's locked rows instead of skipping them, and N consumers would deliver the
        // throughput of one. Without UPDLOCK they would instead overlap, delivering duplicates.
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        const int MessageCount = 60;
        MessageEnvelope[] messages =
            [.. Enumerable.Range(0, MessageCount).Select(i => TestHelpers.Message($"m{i}"))];

        await _fixture.PublishAsync(topic, messages);

        const int ConsumerCount = 6;
        Task<IReadOnlyList<ReceivedMessage>>[] consumers =
        [
            .. Enumerable.Range(0, ConsumerCount).Select(i =>
                Task.Run(() => _fixture.ReceiveAsync(
                    topic, subscription, maxMessages: 10, receiverId: $"consumer-{i}")))
        ];

        IReadOnlyList<ReceivedMessage>[] batches = await Task.WhenAll(consumers);

        List<long> allSequenceNumbers =
            [.. batches.SelectMany(b => b).Select(m => m.Message.SequenceNumber)];

        allSequenceNumbers.Distinct().Count().ShouldBe(
            allSequenceNumbers.Count,
            "no message may be handed to two consumers at once");

        allSequenceNumbers.Count.ShouldBe(
            MessageCount,
            "every message should have been claimed exactly once");
    }

    [Fact]
    public async Task Settlement_with_a_stale_lock_token_is_rejected()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message());

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        SettlementResult result = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .CompleteAsync(received[0].DeliveryId, Guid.NewGuid()));

        result.ShouldBe(SettlementResult.LockLost);
    }

    [Fact]
    public async Task Settling_a_message_that_does_not_exist_reports_not_found()
    {
        SettlementResult result = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().CompleteAsync(-1, Guid.NewGuid()));

        result.ShouldBe(SettlementResult.NotFound);
    }

    [Fact]
    public async Task A_completed_message_is_never_delivered_again()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message());

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .CompleteAsync(received[0].DeliveryId, received[0].LockToken));

        // Even after the lock would have expired, a completed message must not come back.
        _fixture.Clock.Advance(TimeSpan.FromMinutes(5));
        await _fixture.SweepAsync();

        (await _fixture.ReceiveAsync(topic, subscription)).ShouldBeEmpty();
    }

    [Fact]
    public async Task An_expired_lock_returns_the_message_and_counts_the_attempt()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { LockDuration = TimeSpan.FromSeconds(30) });

        await _fixture.PublishAsync(topic, TestHelpers.Message("retry-me"));

        IReadOnlyList<ReceivedMessage> first = await _fixture.ReceiveAsync(topic, subscription);
        first[0].Message.DeliveryCount.ShouldBe(1);

        // The consumer crashes without settling; the lock lapses.
        _fixture.Clock.Advance(TimeSpan.FromSeconds(31));
        await _fixture.SweepAsync();

        IReadOnlyList<ReceivedMessage> second = await _fixture.ReceiveAsync(topic, subscription);

        second.Count.ShouldBe(1);
        second[0].Message.BodyText().ShouldBe("retry-me");
        second[0].Message.DeliveryCount.ShouldBe(
            2,
            "a lapsed attempt still counts, or a consumer that always crashes would retry forever");
    }

    [Fact]
    public async Task Renewing_a_lock_keeps_the_message_held()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { LockDuration = TimeSpan.FromSeconds(30) });

        await _fixture.PublishAsync(topic, TestHelpers.Message());
        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        _fixture.Clock.Advance(TimeSpan.FromSeconds(20));

        DateTimeOffset? renewed = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .RenewLockAsync(received[0].DeliveryId, received[0].LockToken));

        renewed.ShouldNotBeNull();

        // Past the original expiry, but inside the renewed window.
        _fixture.Clock.Advance(TimeSpan.FromSeconds(20));
        await _fixture.SweepAsync();

        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("the renewed lock still holds the message");
    }

    [Fact]
    public async Task Renewing_a_lapsed_lock_fails_rather_than_reviving_it()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions { LockDuration = TimeSpan.FromSeconds(30) });

        await _fixture.PublishAsync(topic, TestHelpers.Message());
        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        _fixture.Clock.Advance(TimeSpan.FromSeconds(31));

        DateTimeOffset? renewed = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .RenewLockAsync(received[0].DeliveryId, received[0].LockToken));

        renewed.ShouldBeNull(
            "the message may already be in another receiver's hands; reviving the lock would " +
            "let two consumers settle it");
    }

    [Fact]
    public async Task Abandoning_returns_the_message_immediately()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message("abandoned"));

        IReadOnlyList<ReceivedMessage> first = await _fixture.ReceiveAsync(topic, subscription);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AbandonAsync(first[0].DeliveryId, first[0].LockToken));

        IReadOnlyList<ReceivedMessage> second = await _fixture.ReceiveAsync(topic, subscription);

        second.Count.ShouldBe(1);
        second[0].Message.DeliveryCount.ShouldBe(2);
    }

    [Fact]
    public async Task Abandoning_with_a_delay_withholds_the_message()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message());

        IReadOnlyList<ReceivedMessage> first = await _fixture.ReceiveAsync(topic, subscription);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().AbandonAsync(
                first[0].DeliveryId,
                first[0].LockToken,
                delay: TimeSpan.FromMinutes(2)));

        (await _fixture.ReceiveAsync(topic, subscription))
            .ShouldBeEmpty("a delayed retry keeps a failing message from burning its budget at once");

        _fixture.Clock.Advance(TimeSpan.FromMinutes(3));

        (await _fixture.ReceiveAsync(topic, subscription)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task Abandoning_can_merge_properties_for_the_next_attempt()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(
            properties: new Dictionary<string, object?>(StringComparer.Ordinal) { ["original"] = 1 }));

        IReadOnlyList<ReceivedMessage> first = await _fixture.ReceiveAsync(topic, subscription);

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().AbandonAsync(
                first[0].DeliveryId,
                first[0].LockToken,
                propertiesToModify: new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["lastError"] = "downstream timeout",
                }));

        IReadOnlyList<ReceivedMessage> second = await _fixture.ReceiveAsync(topic, subscription);

        second[0].Message.ApplicationProperties["lastError"].ShouldBe("downstream timeout");
        second[0].Message.ApplicationProperties["original"].ShouldBe(1L, "existing properties survive");
    }
}
