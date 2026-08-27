using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Outbox;

/// <summary>How the inbox behaves.</summary>
public sealed class InboxOptions
{
    /// <summary>Configuration section these bind from.</summary>
    public const string SectionName = "Inbox";

    /// <summary>
    /// How long a processed-message record is kept.
    /// </summary>
    /// <remarks>
    /// This must outlive every route by which the message could come back: the full retry budget,
    /// time spent in the dead-letter queue, and an operator replaying it days later. The default
    /// is deliberately generous, because pruning early reopens the duplicate window silently and
    /// the records are small.
    /// </remarks>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How often expired records are pruned.</summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromHours(1);
}

/// <summary>
/// Wraps a handler so processing the same message twice has no additional effect.
/// </summary>
/// <typeparam name="TMessage">The payload type.</typeparam>
/// <typeparam name="TContext">The application's <see cref="DbContext"/>.</typeparam>
/// <remarks>
/// <para>
/// Delivery is at-least-once, so a handler will occasionally see a message it has already
/// processed — after a lock expiry, a consumer restart, or a settlement whose acknowledgement was
/// lost. This decorator writes a marker in the <em>same transaction</em> as the handler's own
/// changes, so the work and the record of it either both commit or neither does. A crash between
/// them cannot leave the two out of step.
/// </para>
/// <para>
/// Deduplication is enforced by a unique constraint rather than by reading first, because a
/// read-then-write leaves a window in which two concurrent deliveries both find nothing and both
/// proceed. The loser of the insert race is recognised by the constraint violation.
/// </para>
/// <para>
/// This is a fallback, not the first choice. An operation that is naturally idempotent — an upsert
/// keyed on a business identifier, a write that sets an absolute value — needs none of this
/// bookkeeping.
/// </para>
/// </remarks>
public sealed class IdempotentHandler<TMessage, TContext> : IMessageHandler<TMessage>
    where TContext : DbContext
{
    private readonly IMessageHandler<TMessage> _inner;
    private readonly TContext _context;
    private readonly TimeProvider _time;
    private readonly InboxOptions _options;
    private readonly ILogger<IdempotentHandler<TMessage, TContext>> _logger;
    private readonly string _consumer;

    /// <summary>Creates the decorator.</summary>
    /// <param name="inner">The handler doing the real work.</param>
    /// <param name="context">The database the handler writes to; the marker joins its transaction.</param>
    /// <param name="time">Clock.</param>
    /// <param name="options">Retention settings.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="consumer">
    /// Identifies this consumer. Part of the deduplication key, because the same message
    /// legitimately reaches several subscriptions and one having handled it says nothing about
    /// the others. Defaults to the inner handler's type name.
    /// </param>
    public IdempotentHandler(
        IMessageHandler<TMessage> inner,
        TContext context,
        TimeProvider time,
        IOptions<InboxOptions> options,
        ILogger<IdempotentHandler<TMessage, TContext>> logger,
        string? consumer = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(options);

        _inner = inner;
        _context = context;
        _time = time;
        _options = options.Value;
        _logger = logger;
        _consumer = consumer ?? inner.GetType().Name;
    }

    /// <inheritdoc />
    public async Task HandleAsync(
        MessageContext<TMessage> context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        DateTimeOffset now = _time.GetUtcNow();

        // A cheap pre-check that skips the common repeat without a failed insert. It is an
        // optimisation only — the unique constraint below is what actually guarantees exclusion.
        bool alreadyProcessed = await _context.Set<InboxMessage>()
            .AsNoTracking()
            .AnyAsync(
                m => m.MessageId == context.MessageId && m.Consumer == _consumer,
                cancellationToken);

        if (alreadyProcessed)
        {
            OutboxLog.DuplicateSkipped(_logger, context.MessageId, _consumer);
            return;
        }

        _context.Set<InboxMessage>().Add(new InboxMessage
        {
            MessageId = context.MessageId,
            Consumer = _consumer,
            ProcessedAt = now,
            ExpiresAt = now.Add(_options.Retention),
        });

        // The handler's own writes accumulate on the same context, so the marker and the work
        // commit together in the SaveChangesAsync below.
        await _inner.HandleAsync(context, cancellationToken);

        try
        {
            await _context.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            // Another delivery of this message committed first. Its work is done, so this attempt
            // discards its own — completing rather than retrying, which would loop forever.
            _context.ChangeTracker.Clear();
            OutboxLog.DuplicateSkipped(_logger, context.MessageId, _consumer);
        }
    }

    /// <summary>
    /// Whether a database failure was the inbox's unique constraint rather than something else.
    /// </summary>
    /// <remarks>
    /// Matching on the SQL Server error numbers for a duplicate key (2601, 2627) keeps a genuine
    /// failure — a constraint violation in the handler's own writes, say — from being silently
    /// swallowed as if it were a duplicate.
    /// </remarks>
    private static bool IsDuplicateKey(DbUpdateException exception)
    {
        for (Exception? current = exception.InnerException; current is not null; current = current.InnerException)
        {
            if (current.GetType().Name != "SqlException")
            {
                continue;
            }

            object? number = current.GetType().GetProperty("Number")?.GetValue(current);
            if (number is int code && code is 2601 or 2627)
            {
                return true;
            }
        }

        return false;
    }
}

/// <summary>Prunes inbox records once they can no longer be needed.</summary>
public sealed class InboxCleanupService<TContext> : Microsoft.Extensions.Hosting.BackgroundService
    where TContext : DbContext
{
    private readonly Microsoft.Extensions.DependencyInjection.IServiceScopeFactory _scopeFactory;
    private readonly TimeProvider _time;
    private readonly InboxOptions _options;

    /// <summary>Creates the service.</summary>
    public InboxCleanupService(
        Microsoft.Extensions.DependencyInjection.IServiceScopeFactory scopeFactory,
        TimeProvider time,
        IOptions<InboxOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _scopeFactory = scopeFactory;
        _time = time;
        _options = options.Value;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using PeriodicTimer timer = new(_options.PruneInterval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await using Microsoft.Extensions.DependencyInjection.AsyncServiceScope scope =
                Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions
                    .CreateAsyncScope(_scopeFactory);

            TContext context = Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions
                .GetRequiredService<TContext>(scope.ServiceProvider);

            DateTimeOffset now = _time.GetUtcNow();

            await context.Set<InboxMessage>()
                .Where(m => m.ExpiresAt <= now)
                .ExecuteDeleteAsync(stoppingToken);
        }
    }
}
