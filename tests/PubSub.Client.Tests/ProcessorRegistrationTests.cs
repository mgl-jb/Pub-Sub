using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PubSub.Abstractions;
using PubSub.Client;

namespace PubSub.Client.Tests;

/// <summary>A payload for registration tests; never actually dispatched.</summary>
public sealed record TestMessage(string Value);

/// <summary>A handler for registration tests; never actually invoked.</summary>
public sealed class TestHandler : IMessageHandler<TestMessage>
{
    /// <inheritdoc />
    public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

public class ProcessorRegistrationTests
{
    private static ServiceCollection CreateServices()
    {
        ServiceCollection services = new();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.None));
        services.AddPubSubClient(options => options.BrokerUri = new Uri("http://localhost:8080"));
        return services;
    }

    [Fact]
    public async Task Every_registered_processor_is_hosted()
    {
        // AddHostedService uses TryAddEnumerable, which deduplicates by implementation type. Since
        // every processor shares one host type, using it would silently discard all but the first
        // — a worker consuming four subscriptions would start one and appear to hang on the rest,
        // with no error anywhere. This is that regression.
        ServiceCollection services = CreateServices();

        services.AddMessageProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "shipping";
            options.Handlers.Add<TestMessage, TestHandler>("TestMessage");
        });

        services.AddMessageProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "high-value";
            options.Handlers.Add<TestMessage, TestHandler>("TestMessage");
        });

        services.AddMessageProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "validation";
            options.Handlers.Add<TestMessage, TestHandler>("TestMessage");
        });

        await using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetServices<IHostedService>().Count().ShouldBe(
            3,
            "each subscription needs its own processor running");
    }

    [Fact]
    public async Task Message_and_session_processors_coexist()
    {
        ServiceCollection services = CreateServices();

        services.AddMessageProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "shipping";
            options.Handlers.Add<TestMessage, TestHandler>("TestMessage");
        });

        services.AddSessionProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "customer-timeline";
            options.Handlers.Add<TestMessage, TestHandler>("TestMessage");
        });

        await using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetServices<IHostedService>().Count().ShouldBe(2);
    }

    [Fact]
    public async Task A_processor_with_no_handlers_fails_at_startup()
    {
        // Failing here beats dead-lettering every message at runtime for want of a handler.
        ServiceCollection services = CreateServices();

        services.AddMessageProcessor(options =>
        {
            options.Topic = "orders";
            options.Subscription = "shipping";
        });

        await using ServiceProvider provider = services.BuildServiceProvider();

        InvalidOperationException error = Should.Throw<InvalidOperationException>(
            () => provider.GetServices<IHostedService>().ToList());

        error.Message.ShouldContain("no handlers");
    }

    [Fact]
    public void Each_processor_keeps_its_own_handlers()
    {
        // The registry is per-processor because one worker commonly consumes several
        // subscriptions of the same topic, where the same subject needs a different handler.
        MessageProcessorOptions shipping = new() { Topic = "orders", Subscription = "shipping" };
        MessageProcessorOptions highValue = new() { Topic = "orders", Subscription = "high-value" };

        shipping.Handlers.Add<TestMessage, TestHandler>("OrderPlaced");

        shipping.Handlers.Subjects.ShouldContain("OrderPlaced");
        highValue.Handlers.Subjects.ShouldBeEmpty("registries must not be shared");
    }
}
