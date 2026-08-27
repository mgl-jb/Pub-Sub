using System.Net.Http.Json;
using System.Text.Json;

namespace PubSub.E2E.Tests;

/// <summary>An order, standing in for whatever an application actually publishes.</summary>
public sealed record OrderPlaced(string OrderId, string Region, decimal Total);

/// <summary>
/// Exercises the whole stack over HTTP: the real API, the real client, a real database.
/// </summary>
[Collection(BrokerApiCollection.Name)]
public class EndToEndTests
{
    private readonly BrokerApiFixture _fixture;

    public EndToEndTests(BrokerApiFixture fixture) => _fixture = fixture;

    private HttpClient CreateHttpClient() => _fixture.CreateClient();

    /// <summary>
    /// The running test's cancellation token, so a hung request fails the test promptly rather
    /// than stalling the whole suite until its outer timeout.
    /// </summary>
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_message_published_over_http_is_received_over_http()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        const string Subscription = "all";

        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);
        await CreateSubscriptionAsync(http, topic, Subscription);

        HttpResponseMessage publish = await http.PostAsJsonAsync(
            $"topics/{topic}/messages",
            new
            {
                messages = new[]
                {
                    new
                    {
                        subject = "OrderPlaced",
                        body = Convert.ToBase64String("""{"orderId":"o-1"}"""u8.ToArray()),
                        applicationProperties = new Dictionary<string, object?> { ["region"] = "emea" },
                    },
                },
            },
            Ct);

        publish.EnsureSuccessStatusCode();

        HttpResponseMessage receive = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/{Subscription}/messages/receive",
            new { maxMessages = 10, maxWaitTime = "00:00:02" }, Ct);

        receive.EnsureSuccessStatusCode();

        JsonDocument body = JsonDocument.Parse(await receive.Content.ReadAsStringAsync(Ct));
        JsonElement messages = body.RootElement.GetProperty("messages");

        messages.GetArrayLength().ShouldBe(1);
        messages[0].GetProperty("message").GetProperty("subject").GetString().ShouldBe("OrderPlaced");
    }

    [Fact]
    public async Task Subscription_filters_route_over_the_real_api()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);

        await http.PutAsJsonAsync(
            $"topics/{topic}/subscriptions/high-value",
            new { rule = new { name = "high", sqlExpression = "total > 500" } }, Ct);

        await http.PutAsJsonAsync(
            $"topics/{topic}/subscriptions/emea",
            new { rule = new { name = "emea", sqlExpression = "region = 'emea'" } }, Ct);

        await PublishAsync(http, topic, new Dictionary<string, object?>
        {
            ["total"] = 1000,
            ["region"] = "apac",
        });

        (await ReceiveCountAsync(http, topic, "high-value")).ShouldBe(1);
        (await ReceiveCountAsync(http, topic, "emea")).ShouldBe(0);
    }

    [Fact]
    public async Task Settling_over_http_removes_the_message()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);
        await CreateSubscriptionAsync(http, topic, "all");
        await PublishAsync(http, topic, []);

        JsonElement claimed = await ReceiveOneAsync(http, topic, "all");

        long deliveryId = claimed.GetProperty("deliveryId").GetInt64();
        string lockToken = claimed.GetProperty("lockToken").GetString()!;

        HttpResponseMessage complete = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/all/messages/{deliveryId}/complete",
            new { lockToken }, Ct);

        complete.EnsureSuccessStatusCode();

        (await ReceiveCountAsync(http, topic, "all")).ShouldBe(0);
    }

    [Fact]
    public async Task Settling_with_a_stale_token_returns_conflict()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);
        await CreateSubscriptionAsync(http, topic, "all");
        await PublishAsync(http, topic, []);

        JsonElement claimed = await ReceiveOneAsync(http, topic, "all");
        long deliveryId = claimed.GetProperty("deliveryId").GetInt64();

        HttpResponseMessage complete = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/all/messages/{deliveryId}/complete",
            new { lockToken = Guid.NewGuid() }, Ct);

        // 409 rather than 500: the caller lost a race, which is a state they can reason about.
        complete.StatusCode.ShouldBe(System.Net.HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task A_malformed_filter_is_rejected_when_the_rule_is_created()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);

        HttpResponseMessage response = await http.PutAsJsonAsync(
            $"topics/{topic}/subscriptions/broken",
            new { rule = new { name = "bad", sqlExpression = "total > > 5" } }, Ct);

        response.StatusCode.ShouldBe(
            System.Net.HttpStatusCode.BadRequest,
            "the author of the rule should hear about it, not a subscriber wondering where their " +
            "messages went");
    }

    [Fact]
    public async Task Health_endpoints_report_the_broker_and_its_database()
    {
        HttpClient http = CreateHttpClient();

        (await http.GetAsync(new Uri("health/live", UriKind.Relative), Ct))
            .EnsureSuccessStatusCode();

        (await http.GetAsync(new Uri("health/ready", UriKind.Relative), Ct))
            .EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task An_unknown_topic_returns_not_found()
    {
        HttpClient http = CreateHttpClient();

        HttpResponseMessage response = await http.PostAsJsonAsync(
            "topics/does-not-exist/messages",
            new { messages = new[] { new { body = Convert.ToBase64String("x"u8.ToArray()) } } },
            Ct);

        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task The_dead_letter_queue_is_reachable_over_http()
    {
        string topic = BrokerApiFixture.UniqueName("t");
        HttpClient http = CreateHttpClient();

        await CreateTopicAsync(http, topic);
        await http.PutAsJsonAsync(
            $"topics/{topic}/subscriptions/dlq-test",
            new { maxDeliveryCount = 1 }, Ct);

        await PublishAsync(http, topic, []);

        JsonElement claimed = await ReceiveOneAsync(http, topic, "dlq-test");
        long deliveryId = claimed.GetProperty("deliveryId").GetInt64();
        string lockToken = claimed.GetProperty("lockToken").GetString()!;

        await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/dlq-test/messages/{deliveryId}/abandon",
            new { lockToken }, Ct);

        HttpResponseMessage dlq = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/dlq-test/dead-letter/receive",
            new { maxMessages = 10 }, Ct);

        dlq.EnsureSuccessStatusCode();

        JsonDocument body = JsonDocument.Parse(await dlq.Content.ReadAsStringAsync(Ct));
        body.RootElement.GetProperty("messages").GetArrayLength().ShouldBe(1);

        HttpResponseMessage replay = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/dlq-test/dead-letter/replay",
            new { maxCount = 10 }, Ct);

        replay.EnsureSuccessStatusCode();

        (await ReceiveCountAsync(http, topic, "dlq-test")).ShouldBe(1);
    }

    // --- helpers ---

    private static async Task CreateTopicAsync(HttpClient http, string topic) =>
        (await http.PutAsJsonAsync($"topics/{topic}", new { })).EnsureSuccessStatusCode();

    private static async Task CreateSubscriptionAsync(HttpClient http, string topic, string subscription) =>
        (await http.PutAsJsonAsync($"topics/{topic}/subscriptions/{subscription}", new { }))
        .EnsureSuccessStatusCode();

    private static async Task PublishAsync(
        HttpClient http,
        string topic,
        Dictionary<string, object?> properties)
    {
        HttpResponseMessage response = await http.PostAsJsonAsync(
            $"topics/{topic}/messages",
            new
            {
                messages = new[]
                {
                    new
                    {
                        subject = "OrderPlaced",
                        body = Convert.ToBase64String("""{"orderId":"o-1"}"""u8.ToArray()),
                        applicationProperties = properties,
                    },
                },
            });

        response.EnsureSuccessStatusCode();
    }

    private static async Task<int> ReceiveCountAsync(HttpClient http, string topic, string subscription)
    {
        HttpResponseMessage response = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/{subscription}/messages/receive",
            new { maxMessages = 10, maxWaitTime = "00:00:01" }, Ct);

        response.EnsureSuccessStatusCode();

        JsonDocument body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct));
        return body.RootElement.GetProperty("messages").GetArrayLength();
    }

    private static async Task<JsonElement> ReceiveOneAsync(HttpClient http, string topic, string subscription)
    {
        HttpResponseMessage response = await http.PostAsJsonAsync(
            $"topics/{topic}/subscriptions/{subscription}/messages/receive",
            new { maxMessages = 1, maxWaitTime = "00:00:02" }, Ct);

        response.EnsureSuccessStatusCode();

        JsonDocument body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct));
        JsonElement messages = body.RootElement.GetProperty("messages");

        messages.GetArrayLength().ShouldBeGreaterThan(0, "a message should have been available");
        return messages[0].Clone();
    }
}
