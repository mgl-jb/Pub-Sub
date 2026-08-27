using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

// A small operator tool over the broker's admin and dead-letter endpoints: list what exists, look
// at what failed, and put it back once the cause is fixed.

string brokerUri = Environment.GetEnvironmentVariable("PUBSUB_BROKER_URI") ?? "http://localhost:8080";

using HttpClient http = new() { BaseAddress = new Uri(brokerUri, UriKind.Absolute) };

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

try
{
    return args[0] switch
    {
        "topics" => await ListTopicsAsync(http),
        "subscriptions" when args.Length >= 2 => await ListSubscriptionsAsync(http, args[1]),
        "rules" when args.Length >= 3 => await ListRulesAsync(http, args[1], args[2]),
        "dlq" when args.Length >= 3 => await PeekDeadLetterAsync(http, args[1], args[2]),
        "replay" when args.Length >= 3 => await ReplayAsync(http, args[1], args[2]),
        _ => Unrecognised(),
    };
}
catch (HttpRequestException ex)
{
    Console.Error.WriteLine($"Could not reach the broker at {brokerUri}: {ex.Message}");
    return 2;
}

static int Unrecognised()
{
    PrintUsage();
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine("""
        pubsub-admin — inspect and repair a PubSub broker

        Usage:
          topics                              List every topic.
          subscriptions <topic>               List a topic's subscriptions.
          rules <topic> <subscription>        List a subscription's rules.
          dlq <topic> <subscription>          Show what is in the dead-letter queue.
          replay <topic> <subscription>       Return dead-lettered messages for another attempt.

        The broker address comes from PUBSUB_BROKER_URI (default http://localhost:8080).
        """);
}

static async Task<int> ListTopicsAsync(HttpClient http)
{
    JsonElement topics = await GetJsonAsync(http, "topics");

    if (topics.GetArrayLength() == 0)
    {
        Console.WriteLine("No topics exist.");
        return 0;
    }

    Console.WriteLine($"{"TOPIC",-32} {"TTL",-16} {"DEDUP",-8} CREATED");

    foreach (JsonElement topic in topics.EnumerateArray())
    {
        Console.WriteLine(string.Format(
            CultureInfo.InvariantCulture,
            "{0,-32} {1,-16} {2,-8} {3}",
            topic.GetProperty("name").GetString(),
            topic.GetProperty("defaultTimeToLive").GetString(),
            topic.GetProperty("duplicateDetectionEnabled").GetBoolean() ? "on" : "off",
            topic.GetProperty("createdAt").GetDateTimeOffset().ToString("u", CultureInfo.InvariantCulture)));
    }

    return 0;
}

static async Task<int> ListSubscriptionsAsync(HttpClient http, string topic)
{
    JsonElement subscriptions = await GetJsonAsync(http, $"topics/{Uri.EscapeDataString(topic)}/subscriptions");

    if (subscriptions.GetArrayLength() == 0)
    {
        Console.WriteLine($"Topic '{topic}' has no subscriptions.");
        return 0;
    }

    Console.WriteLine($"{"SUBSCRIPTION",-32} {"LOCK",-12} {"MAX DELIVERY",-14} SESSIONS");

    foreach (JsonElement subscription in subscriptions.EnumerateArray())
    {
        Console.WriteLine(string.Format(
            CultureInfo.InvariantCulture,
            "{0,-32} {1,-12} {2,-14} {3}",
            subscription.GetProperty("name").GetString(),
            subscription.GetProperty("lockDuration").GetString(),
            subscription.GetProperty("maxDeliveryCount").GetInt32(),
            subscription.GetProperty("requiresSession").GetBoolean() ? "required" : "-"));
    }

    return 0;
}

static async Task<int> ListRulesAsync(HttpClient http, string topic, string subscription)
{
    JsonElement rules = await GetJsonAsync(
        http,
        $"topics/{Uri.EscapeDataString(topic)}/subscriptions/{Uri.EscapeDataString(subscription)}/rules");

    if (rules.GetArrayLength() == 0)
    {
        Console.WriteLine(
            $"'{topic}/{subscription}' has no rules, so it receives nothing. This is almost " +
            "always a misconfiguration.");
        return 0;
    }

    foreach (JsonElement rule in rules.EnumerateArray())
    {
        Console.WriteLine($"{rule.GetProperty("name").GetString()}:");
        Console.WriteLine($"  filter: {rule.GetProperty("filter").GetString()}");

        if (rule.TryGetProperty("action", out JsonElement action) && action.ValueKind == JsonValueKind.String)
        {
            Console.WriteLine($"  action: {action.GetString()}");
        }
    }

    return 0;
}

static async Task<int> PeekDeadLetterAsync(HttpClient http, string topic, string subscription)
{
    HttpResponseMessage response = await http.PostAsJsonAsync(
        $"topics/{Uri.EscapeDataString(topic)}/subscriptions/{Uri.EscapeDataString(subscription)}/dead-letter/receive",
        new { maxMessages = 50 });

    response.EnsureSuccessStatusCode();

    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
    JsonElement messages = document.RootElement.GetProperty("messages");

    if (messages.GetArrayLength() == 0)
    {
        Console.WriteLine($"The dead-letter queue for '{topic}/{subscription}' is empty.");
        return 0;
    }

    Console.WriteLine($"{messages.GetArrayLength()} dead-lettered message(s):");
    Console.WriteLine();

    foreach (JsonElement received in messages.EnumerateArray())
    {
        JsonElement message = received.GetProperty("message");

        Console.WriteLine($"  sequence  {message.GetProperty("sequenceNumber").GetInt64()}");
        Console.WriteLine($"  messageId {message.GetProperty("messageId").GetString()}");
        Console.WriteLine($"  subject   {message.GetProperty("subject").GetString()}");
        Console.WriteLine($"  attempts  {message.GetProperty("deliveryCount").GetInt32()}");
        Console.WriteLine($"  reason    {ReadString(message, "deadLetterReason")}");
        Console.WriteLine($"  detail    {ReadString(message, "deadLetterDescription")}");
        Console.WriteLine();
    }

    Console.WriteLine($"Fix the cause first, then: replay {topic} {subscription}");
    return 0;
}

static async Task<int> ReplayAsync(HttpClient http, string topic, string subscription)
{
    HttpResponseMessage response = await http.PostAsJsonAsync(
        $"topics/{Uri.EscapeDataString(topic)}/subscriptions/{Uri.EscapeDataString(subscription)}/dead-letter/replay",
        new { maxCount = 100 });

    response.EnsureSuccessStatusCode();

    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
    int replayed = document.RootElement.GetProperty("replayed").GetInt32();

    Console.WriteLine(replayed == 0
        ? "Nothing to replay."
        : $"Replayed {replayed} message(s) with a fresh delivery budget.");

    return 0;
}

static async Task<JsonElement> GetJsonAsync(HttpClient http, string path)
{
    HttpResponseMessage response = await http.GetAsync(new Uri(path, UriKind.Relative));
    response.EnsureSuccessStatusCode();

    using JsonDocument document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
    return document.RootElement.Clone();
}

static string ReadString(JsonElement element, string property) =>
    element.TryGetProperty(property, out JsonElement value) && value.ValueKind == JsonValueKind.String
        ? value.GetString() ?? "-"
        : "-";
