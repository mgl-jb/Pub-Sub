using StackExchange.Redis;

namespace PubSub.Broker.Redis;

/// <summary>
/// The broker's Redis connection, which may legitimately be absent.
/// </summary>
/// <remarks>
/// Wrapping the multiplexer rather than registering a nullable service makes the optionality part
/// of the type: every consumer has to decide what to do when Redis is missing, instead of
/// discovering it through a null reference. <see cref="IsAvailable"/> is the single check they
/// need, and it covers both "never configured" and "configured but currently disconnected".
/// </remarks>
public sealed class RedisConnection : IDisposable
{
    /// <summary>Creates a connection wrapper, or an empty one when Redis is not configured.</summary>
    public RedisConnection(IConnectionMultiplexer? multiplexer) => Multiplexer = multiplexer;

    /// <summary>An instance representing "no Redis".</summary>
    public static RedisConnection None { get; } = new(null);

    /// <summary>The underlying connection, if any.</summary>
    public IConnectionMultiplexer? Multiplexer { get; }

    /// <summary>Whether Redis can be used right now.</summary>
    public bool IsAvailable => Multiplexer is { IsConnected: true };

    /// <inheritdoc />
    public void Dispose() => Multiplexer?.Dispose();
}
