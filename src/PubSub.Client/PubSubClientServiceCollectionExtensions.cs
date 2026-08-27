using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>Registers the PubSub client.</summary>
public static class PubSubClientServiceCollectionExtensions
{
    /// <summary>
    /// Registers the publisher, the handler registry, and a resilient HTTP client for the broker.
    /// </summary>
    public static IServiceCollection AddPubSubClient(
        this IServiceCollection services,
        Action<PubSubClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        return services.AddPubSubClientCore();
    }

    /// <summary>Registers the client, binding options from configuration.</summary>
    public static IServiceCollection AddPubSubClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<PubSubClientOptions>(
            configuration.GetSection(PubSubClientOptions.SectionName));

        return services.AddPubSubClientCore();
    }

    private static IServiceCollection AddPubSubClientCore(this IServiceCollection services)
    {
        services.TryAddSingleton<MessageTypeRegistry>();
        services.TryAddSingleton<MessageDispatcher>();
        services.TryAddSingleton<IEventPublisher, EventPublisher>();

        services.AddHttpClient<BrokerHttpClient>((provider, http) =>
            {
                PubSubClientOptions options =
                    provider.GetRequiredService<IOptions<PubSubClientOptions>>().Value;

                http.BaseAddress = options.BrokerUri
                                   ?? throw new InvalidOperationException(
                                       "PubSubClientOptions.BrokerUri is not configured.");

                // The per-request timeout must exceed the long-poll wait, or every idle receive
                // would look like a broker failure and trigger a pointless retry.
                http.Timeout = options.RequestTimeout + TimeSpan.FromSeconds(60);
            })
            .AddStandardResilienceHandler(resilience =>
            {
                // A receive that long-polls for 30 seconds is healthy, so the attempt timeout has
                // to allow for it rather than cutting it short.
                resilience.AttemptTimeout.Timeout = TimeSpan.FromSeconds(100);
                resilience.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(300);
                resilience.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(200);
            });

        return services;
    }

    /// <summary>Maps a payload type to a subject for publishing.</summary>
    public static IServiceCollection MapMessageType<T>(
        this IServiceCollection services,
        string? subject = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IConfigureMessageTypes>(
            new ConfigureMessageTypes(registry => registry.Register<T>(subject)));

        return services;
    }

    /// <summary>
    /// Runs a processor for one subscription as a hosted service.
    /// </summary>
    /// <remarks>
    /// Call this once per subscription. Two processors on the same subscription within one process
    /// is legitimate — they simply compete like any other pair of consumers.
    /// </remarks>
    public static IServiceCollection AddMessageProcessor(
        this IServiceCollection services,
        Action<MessageProcessorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddHostedService(provider =>
        {
            MessageProcessorOptions options = new() { Topic = string.Empty, Subscription = string.Empty };
            configure(options);

            ArgumentException.ThrowIfNullOrWhiteSpace(options.Topic, nameof(options.Topic));
            ArgumentException.ThrowIfNullOrWhiteSpace(options.Subscription, nameof(options.Subscription));

            ApplyMessageTypes(provider);

            if (options.Handlers.Subjects.Count == 0)
            {
                throw new InvalidOperationException(
                    $"The processor for '{options.Topic}/{options.Subscription}' has no handlers. " +
                    "Register at least one with options.Handlers.Add<TMessage, THandler>().");
            }

            MessageProcessor processor = new(
                provider.GetRequiredService<BrokerHttpClient>(),
                provider.GetRequiredService<MessageDispatcher>(),
                options,
                provider.GetRequiredService<IOptions<PubSubClientOptions>>(),
                provider.GetRequiredService<ILogger<MessageProcessor>>());

            return new MessageProcessorHost(processor);
        });

        return services;
    }

    /// <summary>Runs a session processor for one session-enabled subscription.</summary>
    public static IServiceCollection AddSessionProcessor(
        this IServiceCollection services,
        Action<SessionProcessorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddHostedService(provider =>
        {
            SessionProcessorOptions options = new() { Topic = string.Empty, Subscription = string.Empty };
            configure(options);

            ArgumentException.ThrowIfNullOrWhiteSpace(options.Topic, nameof(options.Topic));
            ArgumentException.ThrowIfNullOrWhiteSpace(options.Subscription, nameof(options.Subscription));

            ApplyMessageTypes(provider);

            if (options.Handlers.Subjects.Count == 0)
            {
                throw new InvalidOperationException(
                    $"The session processor for '{options.Topic}/{options.Subscription}' has no " +
                    "handlers. Register at least one with options.Handlers.Add<TMessage, THandler>().");
            }

            SessionProcessor processor = new(
                provider.GetRequiredService<BrokerHttpClient>(),
                provider.GetRequiredService<MessageDispatcher>(),
                options,
                provider.GetRequiredService<IOptions<PubSubClientOptions>>(),
                provider.GetRequiredService<ILogger<SessionProcessor>>());

            return new SessionProcessorHost(processor);
        });

        return services;
    }

    private static void ApplyMessageTypes(IServiceProvider provider)
    {
        MessageTypeRegistry types = provider.GetRequiredService<MessageTypeRegistry>();
        foreach (IConfigureMessageTypes configure in provider.GetServices<IConfigureMessageTypes>())
        {
            configure.Apply(types);
        }
    }
}

/// <summary>Applies a subject mapping to the shared registry.</summary>
public interface IConfigureMessageTypes
{
    /// <summary>Applies the mapping.</summary>
    void Apply(MessageTypeRegistry registry);
}

internal sealed class ConfigureMessageTypes : IConfigureMessageTypes
{
    private readonly Action<MessageTypeRegistry> _configure;

    public ConfigureMessageTypes(Action<MessageTypeRegistry> configure) => _configure = configure;

    public void Apply(MessageTypeRegistry registry) => _configure(registry);
}

/// <summary>Hosts a message processor for the application's lifetime.</summary>
internal sealed class MessageProcessorHost : IHostedService, IAsyncDisposable
{
    private readonly MessageProcessor _processor;

    public MessageProcessorHost(MessageProcessor processor) => _processor = processor;

    public Task StartAsync(CancellationToken cancellationToken) =>
        _processor.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        _processor.StopAsync(cancellationToken);

    public ValueTask DisposeAsync() => _processor.DisposeAsync();
}

/// <summary>Hosts a session processor for the application's lifetime.</summary>
internal sealed class SessionProcessorHost : IHostedService, IAsyncDisposable
{
    private readonly SessionProcessor _processor;

    public SessionProcessorHost(SessionProcessor processor) => _processor = processor;

    public Task StartAsync(CancellationToken cancellationToken) =>
        _processor.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        _processor.StopAsync(cancellationToken);

    public ValueTask DisposeAsync() => _processor.DisposeAsync();
}
