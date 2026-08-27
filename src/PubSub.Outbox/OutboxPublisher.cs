using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Outbox;

/// <summary>How the outbox publisher behaves.</summary>
public sealed class OutboxOptions
{
    /// <summary>Configuration section these bind from.</summary>
    public const string SectionName = "Outbox";

    /// <summary>How often the outbox is polled for pending messages.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>How many messages one pass claims.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>How long a claim is held before another instance may take the row.</summary>
    public TimeSpan ClaimDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Attempts before a message is marked failed and left for an operator.
    /// </summary>
    /// <remarks>
    /// Retrying forever would let one unpublishable message consume the publisher indefinitely,
    /// starving everything behind it.
    /// </remarks>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>Base delay for retry backoff; each attempt roughly doubles it.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Longest backoff between attempts.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long published rows are kept before pruning.</summary>
    public TimeSpan PublishedRetention { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Identifies this publisher instance in claims.</summary>
    public string InstanceId { get; set; } = $"{Environment.MachineName}-{Environment.ProcessId}";
}

/// <summary>
/// Moves staged messages from the application's outbox to the broker.
/// </summary>
/// <typeparam name="TContext">The application's <see cref="DbContext"/>.</typeparam>
/// <remarks>
/// Delivery here is at-least-once, deliberately. A publish that succeeds but whose acknowledgement
/// is lost will be retried, so the same message can reach the broker twice — which is why the
/// message id is carried through from the staged row rather than regenerated, letting the topic's
/// duplicate detection recognise the repeat.
/// </remarks>
public sealed class OutboxPublisher<TContext> : BackgroundService
    where TContext : DbContext
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly TimeProvider _time;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxPublisher<TContext>> _logger;

    /// <summary>Creates the publisher.</summary>
    public OutboxPublisher(
        IServiceScopeFactory scopeFactory,
        TimeProvider time,
        IOptions<OutboxOptions> options,
        ILogger<OutboxPublisher<TContext>> logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        _scopeFactory = scopeFactory;
        _time = time;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using PeriodicTimer timer = new(_options.PollInterval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                // Keep draining while a full batch comes back, so a burst is not spread across
                // one poll interval per batch.
                int published;
                do
                {
                    published = await PublishPendingAsync(stoppingToken);
                }
                while (published == _options.BatchSize && !stoppingToken.IsCancellationRequested);

                await PrunePublishedAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // The publisher must outlive any single failure, or staged messages sit forever.
                OutboxLog.PassFailed(_logger, ex);
            }
        }
    }

    /// <summary>Runs one publish pass. Exposed so tests can drive it deterministically.</summary>
    public async Task<int> PublishPendingAsync(CancellationToken cancellationToken = default)
    {
        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        TContext context = scope.ServiceProvider.GetRequiredService<TContext>();
        IEventPublisher publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

        DateTimeOffset now = _time.GetUtcNow();

        IReadOnlyList<OutboxMessage> claimed = await ClaimAsync(context, now, cancellationToken);

        if (claimed.Count == 0)
        {
            return 0;
        }

        foreach (OutboxMessage message in claimed)
        {
            await PublishOneAsync(context, publisher, message, now, cancellationToken);
        }

        await context.SaveChangesAsync(cancellationToken);
        return claimed.Count;
    }

    /// <summary>
    /// Claims a batch for this instance.
    /// </summary>
    /// <remarks>
    /// The same <c>READPAST</c> pattern the broker uses to hand messages to competing consumers:
    /// several publisher instances can run without sending the same message twice, and without
    /// blocking each other. The claim expires so a crashed instance releases its rows.
    /// </remarks>
    private async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(
        TContext context,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        string claimId = $"{_options.InstanceId}-{Guid.NewGuid():n}";
        DateTimeOffset claimedUntil = now.Add(_options.ClaimDuration);

        int claimed = await context.Set<OutboxMessage>()
            .Where(m => (m.Status == OutboxStatus.Pending
                         || (m.Status == OutboxStatus.InFlight && m.ClaimedUntil != null && m.ClaimedUntil <= now))
                        && m.NextAttemptAt <= now)
            .OrderBy(m => m.Id)
            .Take(_options.BatchSize)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(m => m.Status, OutboxStatus.InFlight)
                    .SetProperty(m => m.ClaimedBy, claimId)
                    .SetProperty(m => m.ClaimedUntil, claimedUntil),
                cancellationToken);

        if (claimed == 0)
        {
            return [];
        }

        return await context.Set<OutboxMessage>()
            .Where(m => m.ClaimedBy == claimId)
            .OrderBy(m => m.Id)
            .ToListAsync(cancellationToken);
    }

    private async Task PublishOneAsync(
        TContext context,
        IEventPublisher publisher,
        OutboxMessage message,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        try
        {
            MessageEnvelope envelope = new()
            {
                // Carried through rather than regenerated, so a retried publish is recognisable
                // as the same message by the topic's duplicate detection.
                MessageId = message.MessageId,
                CorrelationId = message.CorrelationId,
                Subject = message.Subject,
                ContentType = message.ContentType,
                Body = message.Body,
                ApplicationProperties = DeserializeProperties(message.ApplicationPropertiesJson),
                SessionId = message.SessionId,
                ScheduledEnqueueTime = message.ScheduledEnqueueTime,
            };

            await publisher.PublishAsync(message.Topic, envelope, cancellationToken);

            message.Status = OutboxStatus.Published;
            message.PublishedAt = now;
            message.ClaimedBy = null;
            message.ClaimedUntil = null;
            message.LastError = null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            message.AttemptCount++;
            message.LastError = ex.Message;
            message.ClaimedBy = null;
            message.ClaimedUntil = null;

            if (message.AttemptCount >= _options.MaxAttempts)
            {
                message.Status = OutboxStatus.Failed;

                OutboxLog.MessageFailed(
                    _logger, ex, message.MessageId, message.Topic, message.AttemptCount);
            }
            else
            {
                message.Status = OutboxStatus.Pending;
                message.NextAttemptAt = now.Add(BackoffFor(message.AttemptCount));

                OutboxLog.PublishAttemptFailed(
                    _logger, ex, message.MessageId, message.Topic, message.AttemptCount);
            }
        }
    }

    /// <summary>
    /// Exponential backoff, capped.
    /// </summary>
    /// <remarks>
    /// Retrying a broker that is down at the poll interval turns an outage into a stampede; the
    /// cap keeps a long outage from pushing the delay out to hours.
    /// </remarks>
    private TimeSpan BackoffFor(int attempt)
    {
        double seconds = _options.RetryBaseDelay.TotalSeconds * Math.Pow(2, attempt - 1);
        double capped = Math.Min(seconds, _options.MaxRetryDelay.TotalSeconds);
        return TimeSpan.FromSeconds(capped);
    }

    private async Task PrunePublishedAsync(CancellationToken cancellationToken)
    {
        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();
        TContext context = scope.ServiceProvider.GetRequiredService<TContext>();

        DateTimeOffset cutoff = _time.GetUtcNow() - _options.PublishedRetention;

        await context.Set<OutboxMessage>()
            .Where(m => m.Status == OutboxStatus.Published
                        && m.PublishedAt != null
                        && m.PublishedAt <= cutoff)
            .ExecuteDeleteAsync(cancellationToken);
    }

    private static Dictionary<string, object?> DeserializeProperties(string? json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }

        Dictionary<string, object?>? properties =
            JsonSerializer.Deserialize<Dictionary<string, object?>>(json, JsonSerializerOptions.Web);

        return properties is null
            ? new Dictionary<string, object?>(StringComparer.Ordinal)
            : new Dictionary<string, object?>(properties, StringComparer.Ordinal);
    }
}

internal static partial class OutboxLog
{
    [LoggerMessage(
        EventId = 3000,
        Level = LogLevel.Error,
        Message = "The outbox pass failed; it will retry on the next interval.")]
    public static partial void PassFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 3001,
        Level = LogLevel.Warning,
        Message = "Publishing outbox message '{MessageId}' to '{Topic}' failed on attempt {Attempt}; backing off.")]
    public static partial void PublishAttemptFailed(
        ILogger logger, Exception exception, string messageId, string topic, int attempt);

    [LoggerMessage(
        EventId = 3002,
        Level = LogLevel.Error,
        Message = "Outbox message '{MessageId}' to '{Topic}' failed permanently after {Attempt} attempts and needs attention.")]
    public static partial void MessageFailed(
        ILogger logger, Exception exception, string messageId, string topic, int attempt);

    [LoggerMessage(
        EventId = 3003,
        Level = LogLevel.Debug,
        Message = "Message '{MessageId}' was already processed by '{Consumer}'; skipping.")]
    public static partial void DuplicateSkipped(ILogger logger, string messageId, string consumer);
}
