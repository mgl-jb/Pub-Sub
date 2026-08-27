using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

/// <summary>
/// Coordinates which broker instance runs the sweeper.
/// </summary>
/// <remarks>
/// The sweep is idempotent, so several instances running it concurrently is safe rather than
/// harmful — it just wastes work and contends on the same rows. Leadership keeps that cost down.
/// </remarks>
public interface ISweepCoordinator
{
    /// <summary>
    /// Attempts to become the sweeper for roughly <paramref name="leaseDuration"/>.
    /// </summary>
    /// <returns><c>true</c> when this instance should sweep now.</returns>
    Task<bool> TryAcquireLeadershipAsync(TimeSpan leaseDuration, CancellationToken cancellationToken = default);
}

/// <summary>Leadership backed by a SQL application lock.</summary>
/// <remarks>
/// The default, and the fallback when Redis is unavailable. It needs no extra infrastructure: the
/// database the broker already depends on arbitrates, and the lock is released automatically when
/// the holding session ends, so a crashed leader does not block its successor.
/// </remarks>
public sealed class SqlSweepCoordinator : ISweepCoordinator
{
    private readonly BrokerDbContext _context;

    /// <summary>Creates the coordinator.</summary>
    public SqlSweepCoordinator(BrokerDbContext context) => _context = context;

    /// <inheritdoc />
    public async Task<bool> TryAcquireLeadershipAsync(
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        // sp_getapplock returns >= 0 when the lock was granted. A zero timeout means an instance
        // that does not get it moves on immediately rather than queueing behind the leader.
        System.Data.Common.DbConnection connection = _context.Database.GetDbConnection();

        bool opened = false;
        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
            opened = true;
        }

        try
        {
            await using System.Data.Common.DbCommand command = connection.CreateCommand();
            command.CommandText = """
                DECLARE @result int;
                EXEC @result = sp_getapplock
                    @Resource = 'PubSub.DeliverySweeper',
                    @LockMode = 'Exclusive',
                    @LockOwner = 'Session',
                    @LockTimeout = 0;
                SELECT @result;
                """;

            object? result = await command.ExecuteScalarAsync(cancellationToken);
            return result is int code && code >= 0;
        }
        finally
        {
            if (opened)
            {
                await connection.CloseAsync();
            }
        }
    }
}

/// <summary>
/// The background pass that keeps delivery state honest: expired locks, expired messages, stale
/// sessions, and stored rows nobody needs any more.
/// </summary>
/// <remarks>
/// Nothing here is optional. Without it an expired lock never returns its message to the
/// subscription, so a crashed consumer silently strands work; a message past its time to live
/// stays deliverable forever; and settled rows accumulate until they slow the claim query down.
/// Each pass is bounded and idempotent, so a failure costs one interval and nothing else.
/// </remarks>
public sealed class DeliverySweeper : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly TimeProvider _time;
    private readonly BrokerOptions _options;
    private readonly ILogger<DeliverySweeper> _logger;

    /// <summary>Creates the sweeper.</summary>
    public DeliverySweeper(
        IServiceScopeFactory scopeFactory,
        TimeProvider time,
        IOptions<BrokerOptions> options,
        ILogger<DeliverySweeper> logger)
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
        using PeriodicTimer timer = new(_options.SweepInterval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await SweepOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // The sweeper must outlive any single failure: stopping would strand every expired
                // lock in the system until the process restarts.
                BrokerLog.SweepFailed(_logger, ex);
            }
        }
    }

    /// <summary>Runs one sweep pass. Exposed so tests can drive it deterministically.</summary>
    public async Task SweepOnceAsync(CancellationToken cancellationToken = default)
    {
        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        ISweepCoordinator coordinator = scope.ServiceProvider.GetRequiredService<ISweepCoordinator>();

        if (!await coordinator.TryAcquireLeadershipAsync(_options.SweepInterval, cancellationToken))
        {
            return;
        }

        BrokerDbContext context = scope.ServiceProvider.GetRequiredService<BrokerDbContext>();
        IDeliveryNotifier notifier = scope.ServiceProvider.GetRequiredService<IDeliveryNotifier>();

        DateTimeOffset now = _time.GetUtcNow();

        int released = await ReleaseExpiredLocksAsync(context, now, cancellationToken);
        int deadLettered = await DeadLetterExpiredMessagesAsync(context, now, cancellationToken);
        int sessions = await ReleaseExpiredSessionsAsync(context, now, cancellationToken);
        await PruneAsync(context, now, cancellationToken);

        if (released > 0)
        {
            BrokerLog.ExpiredLocksReleased(_logger, released);
        }

        if (deadLettered > 0)
        {
            BrokerLog.ExpiredMessagesDeadLettered(_logger, deadLettered);
        }

        if (sessions > 0)
        {
            BrokerLog.ExpiredSessionsReleased(_logger, sessions);
        }

        // Anything returned to Available is work a waiting receiver could take right now.
        if (released > 0 || sessions > 0)
        {
            await NotifyAffectedSubscriptionsAsync(context, notifier, now, cancellationToken);
        }
    }

    /// <summary>
    /// Returns messages whose peek-lock lapsed to their subscription.
    /// </summary>
    /// <remarks>
    /// The delivery count is deliberately left as it is. The attempt was made and did not settle,
    /// so it counts — otherwise a consumer that reliably crashes mid-message would retry forever
    /// instead of eventually dead-lettering.
    /// </remarks>
    private async Task<int> ReleaseExpiredLocksAsync(
        BrokerDbContext context,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        List<long> ids = await context.Deliveries
            .Where(d => d.State == MessageState.Locked && d.LockedUntil != null && d.LockedUntil <= now)
            .OrderBy(d => d.Id)
            .Take(_options.SweepBatchSize)
            .Select(d => d.Id)
            .ToListAsync(cancellationToken);

        if (ids.Count == 0)
        {
            return 0;
        }

        return await context.Deliveries
            .Where(d => ids.Contains(d.Id))
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.Available)
                    .SetProperty(d => d.AvailableAt, now)
                    .SetProperty(d => d.LockToken, (Guid?)null)
                    .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null)
                    .SetProperty(d => d.LockedBy, (string?)null),
                cancellationToken);
    }

    /// <summary>
    /// Dead-letters or discards messages whose time to live has elapsed.
    /// </summary>
    /// <remarks>
    /// Which of the two happens is the subscription's choice. Dead-lettering is the default because
    /// a message quietly vanishing at its expiry is indistinguishable from a message that was lost.
    /// </remarks>
    private async Task<int> DeadLetterExpiredMessagesAsync(
        BrokerDbContext context,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        List<(long Id, bool DeadLetter)> expired = await context.Deliveries
            .Where(d => (d.State == MessageState.Available
                         || d.State == MessageState.Deferred
                         || d.State == MessageState.Locked)
                        && d.ExpiresAt <= now)
            .OrderBy(d => d.Id)
            .Take(_options.SweepBatchSize)
            .Select(d => new ValueTuple<long, bool>(
                d.Id,
                d.Subscription!.DeadLetterOnMessageExpiration))
            .ToListAsync(cancellationToken);

        if (expired.Count == 0)
        {
            return 0;
        }

        long[] toDeadLetter = [.. expired.Where(e => e.DeadLetter).Select(e => e.Id)];
        long[] toDiscard = [.. expired.Where(e => !e.DeadLetter).Select(e => e.Id)];

        int affected = 0;

        if (toDeadLetter.Length > 0)
        {
            affected += await context.Deliveries
                .Where(d => toDeadLetter.Contains(d.Id))
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(d => d.State, MessageState.DeadLettered)
                        .SetProperty(d => d.DeadLetterReason, DeadLetterReason.TimeToLiveExpired)
                        .SetProperty(d => d.DeadLetterDescription,
                            "The message's time to live elapsed before it was settled.")
                        .SetProperty(d => d.SettledAt, now)
                        .SetProperty(d => d.LockToken, (Guid?)null)
                        .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null),
                    cancellationToken);
        }

        if (toDiscard.Length > 0)
        {
            affected += await context.Deliveries
                .Where(d => toDiscard.Contains(d.Id))
                .ExecuteDeleteAsync(cancellationToken);
        }

        return affected;
    }

    /// <summary>Releases session locks whose holder stopped renewing them.</summary>
    private static async Task<int> ReleaseExpiredSessionsAsync(
        BrokerDbContext context,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        // The row is kept so the session's stored state survives for whoever resumes it; only the
        // holder is cleared.
        return await context.SessionLocks
            .Where(s => s.LockedUntil <= now && s.LockedBy != null)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(s => s.LockedBy, (string?)null),
                cancellationToken);
    }

    /// <summary>
    /// Removes rows nobody needs any more: settled deliveries past their retention window, the
    /// messages whose deliveries have all gone, and lapsed duplicate-detection records.
    /// </summary>
    private async Task PruneAsync(
        BrokerDbContext context,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        DateTimeOffset cutoff = now - _options.CompletedRetention;

        int deliveries = await context.Deliveries
            .Where(d => d.State == MessageState.Completed && d.SettledAt != null && d.SettledAt <= cutoff)
            .Take(_options.SweepBatchSize)
            .ExecuteDeleteAsync(cancellationToken);

        // A message is only removed once no subscription still references it, so pruning can never
        // strip the payload out from under a delivery that is still in flight.
        int messages = await context.Messages
            .Where(m => m.ExpiresAt <= cutoff && !m.Deliveries.Any())
            .Take(_options.SweepBatchSize)
            .ExecuteDeleteAsync(cancellationToken);

        int dedup = await context.DedupEntries
            .Where(d => d.ExpiresAt <= now)
            .Take(_options.SweepBatchSize)
            .ExecuteDeleteAsync(cancellationToken);

        if (deliveries > 0 || messages > 0 || dedup > 0)
        {
            BrokerLog.PrunedRows(_logger, deliveries, messages, dedup);
        }
    }

    private static async Task NotifyAffectedSubscriptionsAsync(
        BrokerDbContext context,
        IDeliveryNotifier notifier,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        List<int> subscriptionIds = await context.Deliveries
            .AsNoTracking()
            .Where(d => d.State == MessageState.Available && d.AvailableAt <= now)
            .Select(d => d.SubscriptionId)
            .Distinct()
            .ToListAsync(cancellationToken);

        foreach (int subscriptionId in subscriptionIds)
        {
            await notifier.NotifyAsync(subscriptionId, cancellationToken);
        }
    }
}
