using System.Text;
using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

/// <summary>
/// Sessions trade throughput for order. These tests pin down both halves of that bargain: strict
/// sequencing within a session, and exclusivity that prevents two consumers interleaving it.
/// </summary>
[Collection(BrokerCollection.Name)]
public class SessionTests
{
    private readonly BrokerFixture _fixture;

    public SessionTests(BrokerFixture fixture) => _fixture = fixture;

    private Task<(string Topic, string Subscription)> CreateSessionSubscriptionAsync() =>
        _fixture.CreateTopicWithSubscriptionAsync(
            subscriptionOptions: new SubscriptionOptions
            {
                RequiresSession = true,
                SessionLockDuration = TimeSpan.FromSeconds(60),
            });

    [Fact]
    public async Task Messages_in_a_session_are_delivered_in_sequence_order()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();

        await _fixture.PublishAsync(
            topic,
            TestHelpers.Message("1", sessionId: "customer-a"),
            TestHelpers.Message("2", sessionId: "customer-a"),
            TestHelpers.Message("3", sessionId: "customer-a"));

        AcceptedSession? session = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "customer-a", "worker-1"));

        session.ShouldNotBeNull();

        IReadOnlyList<ReceivedMessage> received =
            await _fixture.ReceiveAsync(topic, subscription, maxMessages: 10, sessionId: "customer-a");

        received.Select(r => r.Message.BodyText()).ShouldBe(["1", "2", "3"]);
    }

    [Fact]
    public async Task A_locked_session_cannot_be_accepted_by_a_second_consumer()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "exclusive"));

        AcceptedSession? first = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "exclusive", "worker-1"));

        AcceptedSession? second = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "exclusive", "worker-2"));

        first.ShouldNotBeNull();
        second.ShouldBeNull("exclusivity is what makes ordering meaningful");
    }

    [Fact]
    public async Task Racing_consumers_produce_exactly_one_session_owner()
    {
        // The unique index on (SubscriptionId, SessionId) is what arbitrates this, not application
        // logic — so the guarantee holds across broker instances, not just within one process.
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "contended"));

        Task<AcceptedSession?>[] attempts =
        [
            .. Enumerable.Range(0, 8).Select(i => Task.Run(() =>
                _fixture.WithScopeAsync(services =>
                    services.GetRequiredService<BrokerStore>()
                        .AcceptSessionAsync(topic, subscription, "contended", $"worker-{i}"))))
        ];

        AcceptedSession?[] results = await Task.WhenAll(attempts);

        results.Count(r => r is not null).ShouldBe(1);
    }

    [Fact]
    public async Task Different_sessions_are_processed_concurrently()
    {
        // Ordering is per session, not global; otherwise one slow customer would stall every other.
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();

        await _fixture.PublishAsync(
            topic,
            TestHelpers.Message("a1", sessionId: "customer-a"),
            TestHelpers.Message("b1", sessionId: "customer-b"));

        AcceptedSession? a = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "customer-a", "worker-1"));

        AcceptedSession? b = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "customer-b", "worker-2"));

        a.ShouldNotBeNull();
        b.ShouldNotBeNull();
    }

    [Fact]
    public async Task Accepting_without_naming_a_session_takes_the_oldest_available_one()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();

        await _fixture.PublishAsync(
            topic,
            TestHelpers.Message("first", sessionId: "session-early"),
            TestHelpers.Message("second", sessionId: "session-late"));

        AcceptedSession? session = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, sessionId: null, receiverId: "worker-1"));

        session.ShouldNotBeNull();
        session.SessionId.ShouldBe("session-early");
    }

    [Fact]
    public async Task A_released_session_becomes_available_again()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "handover"));

        AcceptedSession? first = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "handover", "worker-1"));

        first.ShouldNotBeNull();

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReleaseSessionAsync(topic, subscription, "handover", first.LockToken));

        AcceptedSession? second = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "handover", "worker-2"));

        second.ShouldNotBeNull();
    }

    [Fact]
    public async Task An_abandoned_session_lock_is_reclaimable_after_it_lapses()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "stalled"));

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "stalled", "crashed-worker"));

        _fixture.Clock.Advance(TimeSpan.FromSeconds(61));

        AcceptedSession? reclaimed = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "stalled", "worker-2"));

        reclaimed.ShouldNotBeNull(
            "a crashed consumer must not hold a session hostage indefinitely");
    }

    [Fact]
    public async Task Session_state_survives_a_handover()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "stateful"));

        AcceptedSession? first = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "stateful", "worker-1"));

        first.ShouldNotBeNull();

        bool stored = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().SetSessionStateAsync(
                topic, subscription, "stateful", first.LockToken,
                Encoding.UTF8.GetBytes("checkpoint-7")));

        stored.ShouldBeTrue();

        await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .ReleaseSessionAsync(topic, subscription, "stateful", first.LockToken));

        AcceptedSession? second = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "stateful", "worker-2"));

        second.ShouldNotBeNull();
        second.State.ShouldNotBeNull();
        Encoding.UTF8.GetString(second.State).ShouldBe("checkpoint-7");
    }

    [Fact]
    public async Task Receiving_without_a_session_is_rejected_on_a_session_subscription()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();

        await Should.ThrowAsync<InvalidOperationForStateException>(
            () => _fixture.ReceiveAsync(topic, subscription));
    }

    [Fact]
    public async Task Accepting_a_session_on_a_non_session_subscription_is_rejected()
    {
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        await Should.ThrowAsync<InvalidOperationForStateException>(() =>
            _fixture.WithScopeAsync(services =>
                services.GetRequiredService<BrokerStore>()
                    .AcceptSessionAsync(topic, subscription, "any")));
    }

    [Fact]
    public async Task A_session_lock_can_be_renewed()
    {
        (string topic, string subscription) = await CreateSessionSubscriptionAsync();
        await _fixture.PublishAsync(topic, TestHelpers.Message(sessionId: "long-running"));

        AcceptedSession? session = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "long-running", "worker-1"));

        session.ShouldNotBeNull();

        _fixture.Clock.Advance(TimeSpan.FromSeconds(45));

        DateTimeOffset? renewed = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .RenewSessionLockAsync(topic, subscription, "long-running", session.LockToken));

        renewed.ShouldNotBeNull();

        _fixture.Clock.Advance(TimeSpan.FromSeconds(45));

        AcceptedSession? stolen = await _fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>()
                .AcceptSessionAsync(topic, subscription, "long-running", "worker-2"));

        stolen.ShouldBeNull("the renewal extended ownership past the original expiry");
    }
}
