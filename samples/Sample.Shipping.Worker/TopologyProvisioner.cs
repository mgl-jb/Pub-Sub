using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Client;

namespace Sample.Shipping.Worker;

/// <summary>
/// Creates the topic and subscriptions this worker consumes from, if they do not already exist.
/// </summary>
/// <remarks>
/// <para>
/// Declaring topology from the consumer keeps a subscription's definition next to the code that
/// depends on it: the filter and the handler cannot drift apart, because they ship together.
/// </para>
/// <para>
/// The admin endpoints are idempotent, so running this on every instance and every restart is
/// safe. It does not rewrite an existing subscription's settings, which means a deliberate
/// operational change is not undone by the next deploy.
/// </para>
/// </remarks>
public sealed class TopologyProvisioner : BackgroundService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly PubSubClientOptions _options;
    private readonly ILogger<TopologyProvisioner> _logger;

    /// <summary>Creates the provisioner.</summary>
    public TopologyProvisioner(
        IHttpClientFactory httpClientFactory,
        IOptions<PubSubClientOptions> options,
        ILogger<TopologyProvisioner> logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using HttpClient http = _httpClientFactory.CreateClient();
        http.BaseAddress = _options.BrokerUri;

        // The broker may still be starting; keep trying rather than failing the worker outright.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProvisionAsync(http, stoppingToken);
                return;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                ProvisionLog.WaitingForBroker(_logger, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task ProvisionAsync(HttpClient http, CancellationToken cancellationToken)
    {
        const string Topic = "orders";

        (await http.PutAsJsonAsync(
            $"topics/{Topic}",
            new
            {
                defaultTimeToLive = "14.00:00:00",

                // Producer retries are expected, so repeated message ids within the window are
                // suppressed rather than becoming duplicate orders.
                duplicateDetectionEnabled = true,
                duplicateDetectionWindow = "00:10:00",
            },
            cancellationToken)).EnsureSuccessStatusCode();

        (await http.PutAsJsonAsync(
            $"topics/{Topic}/subscriptions/shipping",
            new
            {
                lockDuration = "00:01:00",
                maxDeliveryCount = 5,
                rule = new { name = "OrderPlaced", sqlExpression = "sys.Subject = 'OrderPlaced'" },
            },
            cancellationToken)).EnsureSuccessStatusCode();

        (await http.PutAsJsonAsync(
            $"topics/{Topic}/subscriptions/high-value",
            new
            {
                maxDeliveryCount = 5,
                rule = new
                {
                    name = "HighValue",
                    sqlExpression = "sys.Subject = 'OrderPlaced' AND total > 500",
                },
            },
            cancellationToken)).EnsureSuccessStatusCode();

        (await http.PutAsJsonAsync(
            $"topics/{Topic}/subscriptions/validation",
            new
            {
                // A short budget so a genuinely bad message reaches the dead-letter queue quickly
                // rather than being retried ten times first.
                maxDeliveryCount = 3,
                rule = new { name = "OrderPlaced", sqlExpression = "sys.Subject = 'OrderPlaced'" },
            },
            cancellationToken)).EnsureSuccessStatusCode();

        (await http.PutAsJsonAsync(
            $"topics/{Topic}/subscriptions/customer-timeline",
            new
            {
                requiresSession = true,
                sessionLockDuration = "00:01:00",
                rule = new { name = "OrderPlaced", sqlExpression = "sys.Subject = 'OrderPlaced'" },
            },
            cancellationToken)).EnsureSuccessStatusCode();

        ProvisionLog.TopologyReady(_logger, Topic);
    }
}

internal static partial class ProvisionLog
{
    [LoggerMessage(
        EventId = 4100,
        Level = LogLevel.Information,
        Message = "Topic '{Topic}' and its subscriptions are ready.")]
    public static partial void TopologyReady(ILogger logger, string topic);

    [LoggerMessage(
        EventId = 4101,
        Level = LogLevel.Information,
        Message = "Waiting for the broker to become reachable: {Reason}")]
    public static partial void WaitingForBroker(ILogger logger, string reason);
}
