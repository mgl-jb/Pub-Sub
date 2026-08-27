using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>The wire shape of a message, mirroring the broker's contract.</summary>
internal sealed record WireMessage
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public string? Subject { get; init; }
    public string ContentType { get; init; } = "application/json";
    public string? Body { get; init; }
    public Dictionary<string, object?>? ApplicationProperties { get; init; }
    public string? SessionId { get; init; }
    public string? ReplyTo { get; init; }
    public string? ReplyToSessionId { get; init; }
    public string? To { get; init; }
    public DateTimeOffset? ScheduledEnqueueTime { get; init; }
    public TimeSpan? TimeToLive { get; init; }
    public long SequenceNumber { get; init; }
    public DateTimeOffset EnqueuedTime { get; init; }
    public int DeliveryCount { get; init; }
    public string? DeadLetterReason { get; init; }
    public string? DeadLetterDescription { get; init; }
}

internal sealed record WirePublishRequest(IReadOnlyList<WireMessage> Messages);

internal sealed record WirePublishResult(long SequenceNumber, bool WasDuplicate, int MatchedSubscriptions);

internal sealed record WirePublishResponse(IReadOnlyList<WirePublishResult> Results);

internal sealed record WireReceiveRequest
{
    public int MaxMessages { get; init; } = 1;
    public TimeSpan MaxWaitTime { get; init; } = TimeSpan.FromSeconds(30);
    public string? SessionId { get; init; }
    public string? ReceiverId { get; init; }
}

internal sealed record WireReceivedMessage(
    long DeliveryId,
    Guid LockToken,
    DateTimeOffset LockedUntil,
    WireMessage Message);

internal sealed record WireReceiveResponse(IReadOnlyList<WireReceivedMessage> Messages);

internal sealed record WireSettleRequest
{
    public required Guid LockToken { get; init; }
    public Dictionary<string, object?>? PropertiesToModify { get; init; }
    public TimeSpan? Delay { get; init; }
    public string? Reason { get; init; }
    public string? Description { get; init; }
}

internal sealed record WireAcceptSessionRequest(string? SessionId, string? ReceiverId);

internal sealed record WireAcceptedSession(
    string SessionId,
    Guid LockToken,
    DateTimeOffset LockedUntil,
    string? State);

internal sealed record WireRenewResponse(DateTimeOffset LockedUntil);

/// <summary>
/// The typed HTTP surface the rest of the client is built on.
/// </summary>
/// <remarks>
/// Every method translates the broker's status codes into the exception the caller expects, so
/// application code sees <see cref="MessageLockLostException"/> rather than a 409 it has to
/// interpret.
/// </remarks>
internal sealed class BrokerHttpClient
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly HttpClient _http;

    public BrokerHttpClient(HttpClient http) => _http = http;

    public async Task<IReadOnlyList<WirePublishResult>> PublishAsync(
        string topic,
        IReadOnlyList<WireMessage> messages,
        CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            $"topics/{Uri.EscapeDataString(topic)}/messages",
            new WirePublishRequest(messages),
            Json,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);

        WirePublishResponse? result = await response.Content
            .ReadFromJsonAsync<WirePublishResponse>(Json, cancellationToken);

        return result?.Results ?? [];
    }

    public async Task<bool> CancelScheduledAsync(
        string topic,
        long sequenceNumber,
        CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.DeleteAsync(
            new Uri($"topics/{Uri.EscapeDataString(topic)}/scheduled/{sequenceNumber}", UriKind.Relative),
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);

        CancelResult? result = await response.Content
            .ReadFromJsonAsync<CancelResult>(Json, cancellationToken);

        return result?.Cancelled ?? false;
    }

    public async Task<IReadOnlyList<WireReceivedMessage>> ReceiveAsync(
        string topic,
        string subscription,
        WireReceiveRequest request,
        bool fromDeadLetterQueue,
        CancellationToken cancellationToken)
    {
        string path = fromDeadLetterQueue ? "dead-letter/receive" : "messages/receive";

        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, path),
            request,
            Json,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);

        WireReceiveResponse? result = await response.Content
            .ReadFromJsonAsync<WireReceiveResponse>(Json, cancellationToken);

        return result?.Messages ?? [];
    }

    public async Task<IReadOnlyList<WireReceivedMessage>> ReceiveDeferredAsync(
        string topic,
        string subscription,
        IReadOnlyList<long> sequenceNumbers,
        string? receiverId,
        CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, "messages/receive-deferred"),
            new { sequenceNumbers, receiverId },
            Json,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);

        WireReceiveResponse? result = await response.Content
            .ReadFromJsonAsync<WireReceiveResponse>(Json, cancellationToken);

        return result?.Messages ?? [];
    }

    public Task CompleteAsync(
        string topic, string subscription, long deliveryId, Guid lockToken, CancellationToken cancellationToken) =>
        SettleAsync(topic, subscription, deliveryId, "complete",
            new WireSettleRequest { LockToken = lockToken }, cancellationToken);

    public Task AbandonAsync(
        string topic,
        string subscription,
        long deliveryId,
        Guid lockToken,
        IDictionary<string, object?>? propertiesToModify,
        TimeSpan? delay,
        CancellationToken cancellationToken) =>
        SettleAsync(topic, subscription, deliveryId, "abandon", new WireSettleRequest
        {
            LockToken = lockToken,
            PropertiesToModify = propertiesToModify is null
                ? null
                : new Dictionary<string, object?>(propertiesToModify, StringComparer.Ordinal),
            Delay = delay,
        }, cancellationToken);

    public Task DeadLetterAsync(
        string topic,
        string subscription,
        long deliveryId,
        Guid lockToken,
        string reason,
        string? description,
        CancellationToken cancellationToken) =>
        SettleAsync(topic, subscription, deliveryId, "dead-letter", new WireSettleRequest
        {
            LockToken = lockToken,
            Reason = reason,
            Description = description,
        }, cancellationToken);

    public Task DeferAsync(
        string topic, string subscription, long deliveryId, Guid lockToken, CancellationToken cancellationToken) =>
        SettleAsync(topic, subscription, deliveryId, "defer",
            new WireSettleRequest { LockToken = lockToken }, cancellationToken);

    public async Task<DateTimeOffset> RenewLockAsync(
        string topic, string subscription, long deliveryId, Guid lockToken, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, $"messages/{deliveryId}/renew-lock"),
            new WireSettleRequest { LockToken = lockToken },
            Json,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken, lockToken);

        WireRenewResponse? result = await response.Content
            .ReadFromJsonAsync<WireRenewResponse>(Json, cancellationToken);

        return result?.LockedUntil ?? throw new BrokerUnavailableException(
            "The broker did not return a renewed lock expiry.");
    }

    public async Task<WireAcceptedSession?> AcceptSessionAsync(
        string topic,
        string subscription,
        string? sessionId,
        string? receiverId,
        CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, "sessions/accept"),
            new WireAcceptSessionRequest(sessionId, receiverId),
            Json,
            cancellationToken);

        // 204 means every session is busy or empty, which is an ordinary outcome rather than a
        // failure the caller should back off from.
        if (response.StatusCode == HttpStatusCode.NoContent)
        {
            return null;
        }

        await EnsureSuccessAsync(response, cancellationToken);

        return await response.Content.ReadFromJsonAsync<WireAcceptedSession>(Json, cancellationToken);
    }

    public async Task<DateTimeOffset> RenewSessionLockAsync(
        string topic, string subscription, string sessionId, Guid lockToken, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, $"sessions/{Uri.EscapeDataString(sessionId)}/renew"),
            new { lockToken },
            Json,
            cancellationToken);

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            throw new SessionLockLostException(sessionId);
        }

        await EnsureSuccessAsync(response, cancellationToken);

        WireRenewResponse? result = await response.Content
            .ReadFromJsonAsync<WireRenewResponse>(Json, cancellationToken);

        return result?.LockedUntil ?? throw new BrokerUnavailableException(
            "The broker did not return a renewed session expiry.");
    }

    public async Task ReleaseSessionAsync(
        string topic, string subscription, string sessionId, Guid lockToken, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, $"sessions/{Uri.EscapeDataString(sessionId)}/release"),
            new { lockToken },
            Json,
            cancellationToken);

        // Releasing a lock that has already lapsed achieves what the caller wanted, so it is not
        // surfaced as an error.
        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            return;
        }

        await EnsureSuccessAsync(response, cancellationToken);
    }

    private async Task SettleAsync(
        string topic,
        string subscription,
        long deliveryId,
        string action,
        WireSettleRequest request,
        CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.PostAsJsonAsync(
            SubscriptionPath(topic, subscription, $"messages/{deliveryId}/{action}"),
            request,
            Json,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken, request.LockToken);
    }

    private static string SubscriptionPath(string topic, string subscription, string suffix) =>
        $"topics/{Uri.EscapeDataString(topic)}/subscriptions/{Uri.EscapeDataString(subscription)}/{suffix}";

    /// <summary>
    /// Translates a broker response into the exception the caller expects.
    /// </summary>
    /// <remarks>
    /// The distinction that matters is transient versus terminal: a 5xx or a timeout is worth
    /// retrying, a 409 lock conflict is not — the message is already someone else's.
    /// </remarks>
    private static async Task EnsureSuccessAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken,
        Guid? lockToken = null)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        string detail = await ReadDetailAsync(response, cancellationToken);

        throw response.StatusCode switch
        {
            HttpStatusCode.Conflict when lockToken is { } token =>
                new MessageLockLostException(token),

            HttpStatusCode.Conflict =>
                new InvalidOperationForStateException(detail),

            HttpStatusCode.NotFound =>
                new EntityNotFoundException(detail),

            HttpStatusCode.BadRequest =>
                new InvalidOperationForStateException(detail),

            HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden =>
                new PubSubException($"The broker rejected the request: {detail}"),

            _ => (Exception)new BrokerUnavailableException(
                $"The broker returned {(int)response.StatusCode}: {detail}"),
        };
    }

    private static async Task<string> ReadDetailAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        try
        {
            string body = await response.Content.ReadAsStringAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(body))
            {
                return response.ReasonPhrase ?? response.StatusCode.ToString();
            }

            using JsonDocument document = JsonDocument.Parse(body);
            if (document.RootElement.TryGetProperty("detail", out JsonElement detail))
            {
                return detail.GetString() ?? body;
            }

            return body;
        }
        catch (JsonException)
        {
            return response.ReasonPhrase ?? response.StatusCode.ToString();
        }
    }

    private sealed record CancelResult(bool Cancelled);
}
