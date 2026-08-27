using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PubSub.Broker.Core;
using PubSub.Broker.Redis;

namespace PubSub.Broker.Tests;

/// <summary>
/// The central claim of the architecture: Redis accelerates dispatch and nothing more.
/// </summary>
/// <remarks>
/// SQL is the system of record, so a broker with no Redis at all must behave identically apart
/// from latency. These tests run the notifier and the sweep coordinator against a connection that
/// represents "no Redis" — the same state the broker reaches when Redis is down, misconfigured, or
/// simply not deployed — and assert the fallbacks carry the load.
/// </remarks>
[Collection(BrokerCollection.Name)]
public class RedisFallbackTests
{
    private readonly BrokerFixture _fixture;

    public RedisFallbackTests(BrokerFixture fixture) => _fixture = fixture;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static RedisDeliveryNotifier CreateNotifierWithoutRedis() =>
        new(
            RedisConnection.None,
            Options.Create(new RedisOptions()),
            NullLogger<RedisDeliveryNotifier>.Instance);

    [Fact]
    public void An_unconfigured_connection_reports_itself_unavailable() =>
        RedisConnection.None.IsAvailable.ShouldBeFalse();

    [Fact]
    public async Task Notifying_without_redis_still_wakes_local_receivers()
    {
        // The in-process path must keep working, or a single-instance deployment would lose the
        // low-latency dispatch it never needed Redis for in the first place.
        RedisDeliveryNotifier notifier = CreateNotifierWithoutRedis();

        Task<bool> waiter = notifier.WaitAsync(42, TimeSpan.FromSeconds(5), Ct);
        await notifier.NotifyAsync(42, Ct);

        (await waiter).ShouldBeTrue();
    }

    [Fact]
    public async Task Waiting_without_redis_times_out_rather_than_hanging()
    {
        RedisDeliveryNotifier notifier = CreateNotifierWithoutRedis();

        bool signalled = await notifier.WaitAsync(99, TimeSpan.FromMilliseconds(200), Ct);

        signalled.ShouldBeFalse("no signal arrived, so the receiver falls back to polling");
    }

    [Fact]
    public async Task Sweeper_leadership_falls_back_to_the_database()
    {
        // With Redis absent, sp_getapplock arbitrates instead. The database the broker already
        // depends on is enough; no extra infrastructure is required to run the sweeper safely.
        await _fixture.WithScopeAsync(async services =>
        {
            SqlSweepCoordinator sqlCoordinator = new(
                (BrokerDbContext)services.GetService(typeof(BrokerDbContext))!);

            RedisSweepCoordinator coordinator = new(
                RedisConnection.None,
                sqlCoordinator,
                Options.Create(new RedisOptions()));

            bool acquired = await coordinator.TryAcquireLeadershipAsync(TimeSpan.FromSeconds(30), Ct);

            acquired.ShouldBeTrue("a lone instance should be able to sweep");
        });
    }

    [Fact]
    public async Task Publish_and_receive_work_end_to_end_with_no_redis()
    {
        // The fixture never configures Redis, so every other test in this suite already runs on
        // the fallback path. This states that explicitly rather than leaving it implied.
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        await _fixture.PublishAsync(topic, TestHelpers.Message("no-redis-required"));

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(topic, subscription);

        received.Count.ShouldBe(1);
        received[0].Message.BodyText().ShouldBe("no-redis-required");
    }

    [Fact]
    public async Task Long_polling_still_delivers_without_a_wakeup_signal()
    {
        // NullDeliveryNotifier never signals anything, which is the worst case Redis loss can
        // produce. The receiver must still find the message by polling.
        (string topic, string subscription) = await _fixture.CreateTopicWithSubscriptionAsync();

        await _fixture.PublishAsync(topic, TestHelpers.Message("found-by-polling"));

        bool signalled = await NullDeliveryNotifier.Instance.WaitAsync(
            1, TimeSpan.FromMilliseconds(100), Ct);

        signalled.ShouldBeFalse("this notifier deliberately signals nothing");

        IReadOnlyList<ReceivedMessage> received = await _fixture.ReceiveAsync(
            topic, subscription, maxWait: TimeSpan.FromSeconds(2));

        received.Count.ShouldBe(1, "the message is found regardless of whether a signal arrived");
    }
}
