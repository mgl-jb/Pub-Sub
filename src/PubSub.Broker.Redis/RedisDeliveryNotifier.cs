using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Broker.Core;
using StackExchange.Redis;

namespace PubSub.Broker.Redis;

/// <summary>
/// Wakes long-polling receivers across every broker instance when a message arrives.
/// </summary>
/// <remarks>
/// <para>
/// Without this, a receiver attached to instance A never learns about a publish that landed on
/// instance B until its poll interval elapses. Redis pub/sub closes that gap, taking dispatch
/// latency from "up to one poll interval" to "as soon as the publish commits".
/// </para>
/// <para>
/// It is strictly an accelerator. Redis pub/sub is fire-and-forget — a signal published while a
/// subscriber is reconnecting is simply gone — which would be a serious flaw if messages depended
/// on it. They do not: the receiver's timed poll finds the message regardless, so a lost signal
/// costs one interval of latency and nothing else. Every failure below therefore degrades to the
/// in-process notifier rather than propagating.
/// </para>
/// </remarks>
public sealed class RedisDeliveryNotifier : IDeliveryNotifier, IAsyncDisposable
{
    private readonly RedisConnection _redis;
    private readonly InProcessDeliveryNotifier _local = new();
    private readonly RedisOptions _options;
    private readonly ILogger<RedisDeliveryNotifier> _logger;
    private readonly HashSet<int> _subscribed = [];
    private readonly SemaphoreSlim _subscribeLock = new(1, 1);

    /// <summary>Creates the notifier.</summary>
    /// <param name="redis">The connection, which may represent "no Redis".</param>
    /// <param name="options">Redis settings.</param>
    /// <param name="logger">Logger.</param>
    public RedisDeliveryNotifier(
        RedisConnection redis,
        IOptions<RedisOptions> options,
        ILogger<RedisDeliveryNotifier> logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        _redis = redis;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task NotifyAsync(int subscriptionId, CancellationToken cancellationToken = default)
    {
        // Local waiters first: they are woken even if Redis is unreachable.
        await _local.NotifyAsync(subscriptionId, cancellationToken);

        if (!_redis.IsAvailable)
        {
            return;
        }

        try
        {
            await _redis.Multiplexer!.GetSubscriber()
                .PublishAsync(ChannelFor(subscriptionId), RedisValue.EmptyString, CommandFlags.FireAndForget);
        }
        catch (RedisException ex)
        {
            // A signal that never arrives costs a poll interval of latency, so it is not worth
            // failing the publish that triggered it.
            RedisLog.NotifyFailed(_logger, ex, subscriptionId);
        }
    }

    /// <inheritdoc />
    public async Task<bool> WaitAsync(
        int subscriptionId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        await EnsureSubscribedAsync(subscriptionId);
        return await _local.WaitAsync(subscriptionId, timeout, cancellationToken);
    }

    /// <summary>
    /// Subscribes to a subscription's channel once, forwarding remote signals to local waiters.
    /// </summary>
    private async Task EnsureSubscribedAsync(int subscriptionId)
    {
        if (!_redis.IsAvailable)
        {
            return;
        }

        lock (_subscribed)
        {
            if (_subscribed.Contains(subscriptionId))
            {
                return;
            }
        }

        await _subscribeLock.WaitAsync();

        try
        {
            lock (_subscribed)
            {
                if (!_subscribed.Add(subscriptionId))
                {
                    return;
                }
            }

            await _redis.Multiplexer!.GetSubscriber().SubscribeAsync(
                ChannelFor(subscriptionId),
                (_, _) => _ = _local.NotifyAsync(subscriptionId));
        }
        catch (RedisException ex)
        {
            lock (_subscribed)
            {
                _subscribed.Remove(subscriptionId);
            }

            // Falling back to timed polling for this subscription.
            RedisLog.SubscribeFailed(_logger, ex, subscriptionId);
        }
        finally
        {
            _subscribeLock.Release();
        }
    }

    private RedisChannel ChannelFor(int subscriptionId) =>
        RedisChannel.Literal($"{_options.KeyPrefix}:sub:{subscriptionId}");

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _subscribeLock.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Elects one broker instance to run the sweeper, using a Redis lease.
/// </summary>
/// <remarks>
/// The sweep is idempotent, so a lost or duplicated lease is wasteful rather than harmful. That is
/// what makes a plain <c>SET NX EX</c> sufficient here: this is not a distributed lock protecting
/// correctness, and treating it as one would be a mistake.
/// </remarks>
public sealed class RedisSweepCoordinator : ISweepCoordinator
{
    private readonly RedisConnection _redis;
    private readonly SqlSweepCoordinator _fallback;
    private readonly RedisOptions _options;
    private readonly string _instanceId =
        $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():n}";

    /// <summary>Creates the coordinator.</summary>
    /// <param name="redis">The connection, which may represent "no Redis".</param>
    /// <param name="fallback">Used whenever Redis is unavailable.</param>
    /// <param name="options">Redis settings.</param>
    public RedisSweepCoordinator(
        RedisConnection redis,
        SqlSweepCoordinator fallback,
        IOptions<RedisOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _redis = redis;
        _fallback = fallback;
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task<bool> TryAcquireLeadershipAsync(
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        if (!_redis.IsAvailable)
        {
            return await _fallback.TryAcquireLeadershipAsync(leaseDuration, cancellationToken);
        }

        try
        {
            RedisKey key = $"{_options.KeyPrefix}:sweeper:leader";
            IDatabase database = _redis.Multiplexer!.GetDatabase();

            // Taking the lease when it is free.
            if (await database.StringSetAsync(
                    key, _instanceId, _options.LeadershipLease, When.NotExists))
            {
                return true;
            }

            // Or renewing it when this instance already holds it.
            RedisValue holder = await database.StringGetAsync(key);
            if (holder == _instanceId)
            {
                await database.KeyExpireAsync(key, _options.LeadershipLease);
                return true;
            }

            return false;
        }
        catch (RedisException)
        {
            // The database can arbitrate perfectly well on its own.
            return await _fallback.TryAcquireLeadershipAsync(leaseDuration, cancellationToken);
        }
    }
}

internal static partial class RedisLog
{
    [LoggerMessage(
        EventId = 5000,
        Level = LogLevel.Debug,
        Message = "Could not signal subscription {SubscriptionId} through Redis; receivers will find the message on their next poll.")]
    public static partial void NotifyFailed(ILogger logger, Exception exception, int subscriptionId);

    [LoggerMessage(
        EventId = 5001,
        Level = LogLevel.Warning,
        Message = "Could not subscribe to the Redis channel for subscription {SubscriptionId}; falling back to timed polling.")]
    public static partial void SubscribeFailed(ILogger logger, Exception exception, int subscriptionId);

    [LoggerMessage(
        EventId = 5002,
        Level = LogLevel.Warning,
        Message = "Redis is unavailable, so dispatch falls back to polling and the sweeper to a database lock. Correctness is unaffected.")]
    public static partial void RedisUnavailable(ILogger logger, Exception exception);
}
