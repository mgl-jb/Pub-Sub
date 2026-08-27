using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace PubSub.Broker.Core;

/// <summary>Registers the broker's services.</summary>
public static class BrokerServiceCollectionExtensions
{
    /// <summary>
    /// Registers the broker store, administration surface, rule cache, and sweeper.
    /// </summary>
    /// <remarks>
    /// The notifier and sweep coordinator are registered with <c>TryAdd</c> so a caller that has
    /// already registered the Redis implementations keeps them; without Redis the in-process and
    /// SQL-backed defaults take effect, and the broker behaves identically apart from dispatch
    /// latency.
    /// </remarks>
    public static IServiceCollection AddPubSubBroker(
        this IServiceCollection services,
        string connectionString,
        Action<BrokerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.AddDbContext<BrokerDbContext>(options =>
            options.UseSqlServer(connectionString, sql =>
            {
                // The broker's own retry policy sits above this, but transient connection faults
                // are better absorbed here than surfaced as failed publishes.
                sql.EnableRetryOnFailure(
                    maxRetryCount: 5,
                    maxRetryDelay: TimeSpan.FromSeconds(10),
                    errorNumbersToAdd: null);
            }));

        return services.AddPubSubBrokerCore(configure);
    }

    /// <summary>
    /// Registers the broker's services without configuring the database, for callers that
    /// register <see cref="BrokerDbContext"/> themselves.
    /// </summary>
    public static IServiceCollection AddPubSubBrokerCore(
        this IServiceCollection services,
        Action<BrokerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<BrokerOptions>();
        }

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<RuleSetCache>();
        services.TryAddSingleton<IDeliveryNotifier, InProcessDeliveryNotifier>();
        services.TryAddScoped<ISweepCoordinator, SqlSweepCoordinator>();

        services.AddScoped<BrokerStore>();
        services.AddScoped<BrokerAdmin>();

        return services;
    }

    /// <summary>Binds <see cref="BrokerOptions"/> from configuration.</summary>
    public static IServiceCollection AddPubSubBrokerOptions(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<BrokerOptions>(configuration.GetSection(BrokerOptions.SectionName));
        return services;
    }

    /// <summary>Runs the background sweeper in this process.</summary>
    /// <remarks>
    /// Safe to call on every broker instance: leadership is arbitrated at runtime, so only one
    /// actually sweeps at a time.
    /// </remarks>
    public static IServiceCollection AddPubSubSweeper(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddHostedService<DeliverySweeper>();
        return services;
    }
}
