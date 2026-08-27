namespace PubSub.Broker.Redis;

/// <summary>How the broker uses Redis.</summary>
/// <remarks>
/// Every setting here tunes an optimisation. Redis holds no state the broker cannot rebuild from
/// SQL, so misconfiguring these costs latency, never correctness.
/// </remarks>
public sealed class RedisOptions
{
    /// <summary>Configuration section these bind from.</summary>
    public const string SectionName = "Redis";

    /// <summary>The Redis connection string.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Prefixes every key and channel, so several brokers can share one Redis instance.</summary>
    public string KeyPrefix { get; set; } = "pubsub";

    /// <summary>
    /// How long the sweeper leadership lease is held.
    /// </summary>
    /// <remarks>
    /// Comfortably longer than a sweep pass, so the leader does not lose its lease mid-pass; short
    /// enough that a crashed leader is replaced promptly.
    /// </remarks>
    public TimeSpan LeadershipLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to wait for Redis before falling back.</summary>
    /// <remarks>
    /// Deliberately short. A slow Redis must not slow the broker down — the fallback path is
    /// always available and costs only latency.
    /// </remarks>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
