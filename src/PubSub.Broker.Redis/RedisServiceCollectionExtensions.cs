using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Broker.Core;
using StackExchange.Redis;

namespace PubSub.Broker.Redis;

/// <summary>Registers the optional Redis hot path.</summary>
public static class RedisServiceCollectionExtensions
{
    /// <summary>
    /// Adds Redis-backed receiver wakeups and sweeper leadership.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Both are optimisations. If Redis is not configured, or cannot be reached at startup, the
    /// broker registers the in-process notifier and the SQL leader instead and runs correctly —
    /// dispatch simply falls back to polling, which costs latency.
    /// </para>
    /// <para>
    /// The connection is established eagerly here so that a misconfiguration is visible in the
    /// startup log rather than surfacing as unexplained latency later.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddPubSubRedis(
        this IServiceCollection services,
        Action<RedisOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        return services.AddPubSubRedisCore();
    }

    /// <summary>Adds the Redis hot path, binding options from configuration.</summary>
    public static IServiceCollection AddPubSubRedis(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<RedisOptions>(configuration.GetSection(RedisOptions.SectionName));
        return services.AddPubSubRedisCore();
    }

    private static IServiceCollection AddPubSubRedisCore(this IServiceCollection services)
    {
        services.AddSingleton(provider =>
        {
            RedisOptions options = provider.GetRequiredService<IOptions<RedisOptions>>().Value;
            ILogger<RedisDeliveryNotifier> logger =
                provider.GetRequiredService<ILogger<RedisDeliveryNotifier>>();

            if (string.IsNullOrWhiteSpace(options.ConnectionString))
            {
                return RedisConnection.None;
            }

            try
            {
                ConfigurationOptions configuration =
                    ConfigurationOptions.Parse(options.ConnectionString);

                configuration.ConnectTimeout = (int)options.ConnectTimeout.TotalMilliseconds;

                // Starting without Redis is a supported state, so a failure to connect must not
                // prevent the broker from coming up.
                configuration.AbortOnConnectFail = false;

                return new RedisConnection(ConnectionMultiplexer.Connect(configuration));
            }
            catch (RedisConnectionException ex)
            {
                RedisLog.RedisUnavailable(logger, ex);
                return RedisConnection.None;
            }
        });

        // Registered before the core defaults so these win, while the core's TryAdd calls remain
        // the fallback when this method is not used at all.
        services.TryAddSingleton<IDeliveryNotifier, RedisDeliveryNotifier>();
        services.TryAddScoped<SqlSweepCoordinator>();
        services.TryAddScoped<ISweepCoordinator, RedisSweepCoordinator>();

        return services;
    }
}
