using System.Text;
using Microsoft.Extensions.DependencyInjection;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Tests;

/// <summary>Shortcuts for arranging broker state in tests.</summary>
internal static class TestHelpers
{
    /// <summary>Builds a message with a JSON-ish body and optional routing properties.</summary>
    public static MessageEnvelope Message(
        string body = "payload",
        string? subject = null,
        string? sessionId = null,
        string? messageId = null,
        string? correlationId = null,
        Dictionary<string, object?>? properties = null,
        DateTimeOffset? scheduledFor = null,
        TimeSpan? timeToLive = null) =>
        new()
        {
            MessageId = messageId ?? Guid.NewGuid().ToString("n"),
            Subject = subject,
            SessionId = sessionId,
            CorrelationId = correlationId,
            Body = Encoding.UTF8.GetBytes(body),
            ApplicationProperties = properties ?? new Dictionary<string, object?>(StringComparer.Ordinal),
            ScheduledEnqueueTime = scheduledFor,
            TimeToLive = timeToLive,
        };

    /// <summary>Reads a message body back as text.</summary>
    public static string BodyText(this MessageEnvelope message) =>
        Encoding.UTF8.GetString(message.Body.Span);

    /// <summary>Creates a topic and one subscription, returning both names.</summary>
    public static async Task<(string Topic, string Subscription)> CreateTopicWithSubscriptionAsync(
        this BrokerFixture fixture,
        TopicOptions? topicOptions = null,
        SubscriptionOptions? subscriptionOptions = null,
        RuleDescriptor? rule = null)
    {
        string topic = BrokerFixture.UniqueName("t");
        string subscription = BrokerFixture.UniqueName("s");

        await fixture.WithScopeAsync(async services =>
        {
            BrokerAdmin admin = services.GetRequiredService<BrokerAdmin>();
            await admin.CreateTopicAsync(topic, topicOptions);
            await admin.CreateSubscriptionAsync(topic, subscription, subscriptionOptions, rule);
        });

        return (topic, subscription);
    }

    /// <summary>Publishes messages through a fresh scope.</summary>
    public static Task<IReadOnlyList<PublishResult>> PublishAsync(
        this BrokerFixture fixture,
        string topic,
        params MessageEnvelope[] messages) =>
        fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().PublishAsync(topic, messages));

    /// <summary>Receives through a fresh scope, without waiting by default.</summary>
    public static Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
        this BrokerFixture fixture,
        string topic,
        string subscription,
        int maxMessages = 10,
        TimeSpan? maxWait = null,
        string? sessionId = null,
        string? receiverId = null,
        bool fromDeadLetterQueue = false) =>
        fixture.WithScopeAsync(services =>
            services.GetRequiredService<BrokerStore>().ReceiveAsync(new ReceiveRequest
            {
                Topic = topic,
                Subscription = subscription,
                MaxMessages = maxMessages,
                MaxWaitTime = maxWait ?? TimeSpan.Zero,
                SessionId = sessionId,
                ReceiverId = receiverId,
                FromDeadLetterQueue = fromDeadLetterQueue,
            }));

    /// <summary>Runs one sweeper pass synchronously.</summary>
    public static async Task SweepAsync(this BrokerFixture fixture)
    {
        await using AsyncServiceScope scope = fixture.CreateScope();

        DeliverySweeper sweeper = new(
            scope.ServiceProvider.GetRequiredService<IServiceScopeFactory>(),
            fixture.Clock,
            scope.ServiceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<BrokerOptions>>(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DeliverySweeper>.Instance);

        await sweeper.SweepOnceAsync();
    }
}
