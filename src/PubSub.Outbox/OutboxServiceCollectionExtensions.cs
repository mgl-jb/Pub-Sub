using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Outbox;

/// <summary>Registers the outbox and inbox.</summary>
public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Runs the outbox publisher against the application's database.
    /// </summary>
    /// <remarks>
    /// Safe to run on every instance: rows are claimed with the same skip-locked pattern the
    /// broker uses, so instances share the work rather than duplicating it.
    /// </remarks>
    public static IServiceCollection AddPubSubOutbox<TContext>(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<OutboxOptions>();
        }

        services.TryAddSingleton(TimeProvider.System);
        services.AddHostedService<OutboxPublisher<TContext>>();

        return services;
    }

    /// <summary>Binds <see cref="OutboxOptions"/> from configuration.</summary>
    public static IServiceCollection AddPubSubOutboxOptions(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<OutboxOptions>(configuration.GetSection(OutboxOptions.SectionName));
        services.Configure<InboxOptions>(configuration.GetSection(InboxOptions.SectionName));

        return services;
    }

    /// <summary>
    /// Registers a handler wrapped so that reprocessing a message has no additional effect.
    /// </summary>
    /// <remarks>
    /// Reach for this when the handler's work is not naturally idempotent. Where it is — an upsert
    /// keyed on a business identifier, a write that sets an absolute value — register the handler
    /// directly and skip the bookkeeping.
    /// </remarks>
    public static IServiceCollection AddIdempotentHandler<TMessage, THandler, TContext>(
        this IServiceCollection services,
        string? consumerName = null)
        where THandler : class, IMessageHandler<TMessage>
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton(TimeProvider.System);
        services.AddOptions<InboxOptions>();
        services.TryAddScoped<THandler>();

        services.AddScoped<IMessageHandler<TMessage>>(provider => new IdempotentHandler<TMessage, TContext>(
            provider.GetRequiredService<THandler>(),
            provider.GetRequiredService<TContext>(),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetRequiredService<IOptions<InboxOptions>>(),
            provider.GetRequiredService<ILogger<IdempotentHandler<TMessage, TContext>>>(),
            consumerName ?? typeof(THandler).Name));

        return services;
    }

    /// <summary>Prunes inbox records once they can no longer be needed.</summary>
    public static IServiceCollection AddInboxCleanup<TContext>(this IServiceCollection services)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<InboxOptions>();
        services.TryAddSingleton(TimeProvider.System);
        services.AddHostedService<InboxCleanupService<TContext>>();

        return services;
    }
}
