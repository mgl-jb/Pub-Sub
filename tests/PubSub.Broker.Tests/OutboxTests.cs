using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;
using PubSub.Outbox;

namespace PubSub.Broker.Tests;

/// <summary>A trivial application database carrying the outbox and inbox.</summary>
public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options)
    {
    }

    public DbSet<WidgetRecord> Widgets => Set<WidgetRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<WidgetRecord>(entity =>
        {
            entity.ToTable("Widgets");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasMaxLength(64);
        });

        modelBuilder.AddPubSubOutbox();
    }
}

/// <summary>A business record written alongside an outbox entry.</summary>
public sealed class WidgetRecord
{
    public string Id { get; set; } = string.Empty;

    public int Count { get; set; }
}

/// <summary>The payload staged in the outbox.</summary>
public sealed record WidgetCreated(string WidgetId, int Count);

/// <summary>Captures what was published, so a test can assert the outbox drained.</summary>
internal sealed class CapturingPublisher : IEventPublisher
{
    private long _sequence;

    public List<(string Topic, MessageEnvelope Message)> Published { get; } = [];

    /// <summary>When set, every publish throws — used to exercise the retry path.</summary>
    public bool FailEveryPublish { get; set; }

    public Task<long> PublishAsync<T>(
        string topic, T payload, Action<PublishOptions>? configure = null, CancellationToken cancellationToken = default)
        where T : notnull =>
        throw new NotSupportedException("The outbox publishes pre-built envelopes.");

    public Task<long> PublishAsync(string topic, MessageEnvelope message, CancellationToken cancellationToken = default)
    {
        if (FailEveryPublish)
        {
            throw new BrokerUnavailableException("The broker is pretending to be down.");
        }

        Published.Add((topic, message));
        return Task.FromResult(Interlocked.Increment(ref _sequence));
    }

    public Task<IReadOnlyList<long>> PublishBatchAsync(
        string topic, IEnumerable<MessageEnvelope> messages, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public Task<long> ScheduleAsync(
        string topic, MessageEnvelope message, DateTimeOffset enqueueAt, CancellationToken cancellationToken = default) =>
        PublishAsync(topic, message, cancellationToken);

    public Task<bool> CancelScheduledAsync(
        string topic, long sequenceNumber, CancellationToken cancellationToken = default) =>
        Task.FromResult(false);
}

/// <summary>
/// The outbox against a real database, because the guarantee it provides is transactional and a
/// fake would prove nothing about it.
/// </summary>
[Collection(BrokerCollection.Name)]
public class OutboxTests
{
    private readonly BrokerFixture _fixture;

    public OutboxTests(BrokerFixture fixture) => _fixture = fixture;

    /// <summary>The running test's cancellation token, so a hung query fails promptly.</summary>
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<(ServiceProvider Services, CapturingPublisher Publisher)> CreateAppAsync()
    {
        CapturingPublisher publisher = new();

        // A database of its own per test class run. EnsureCreated only creates schema when it
        // creates the database, so pointing at the broker's existing one would silently skip the
        // outbox tables.
        Microsoft.Data.SqlClient.SqlConnectionStringBuilder connection =
            new(_fixture.ConnectionString)
            {
                InitialCatalog = $"app_{Guid.NewGuid():n}",
            };

        ServiceCollection services = new();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(options =>
            options.UseSqlServer(connection.ConnectionString));
        services.AddSingleton<TimeProvider>(_fixture.Clock);
        services.AddSingleton<IEventPublisher>(publisher);
        services.AddOptions<OutboxOptions>();
        services.AddOptions<InboxOptions>();

        ServiceProvider provider = services.BuildServiceProvider();

        await using (AsyncServiceScope scope = provider.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            await db.Database.EnsureCreatedAsync(Ct);
        }

        return (provider, publisher);
    }

    private OutboxPublisher<AppDbContext> CreatePublisher(ServiceProvider services) =>
        new(
            services.GetRequiredService<IServiceScopeFactory>(),
            _fixture.Clock,
            services.GetRequiredService<IOptions<OutboxOptions>>(),
            NullLogger<OutboxPublisher<AppDbContext>>.Instance);

    [Fact]
    public async Task A_staged_message_is_published_after_the_transaction_commits()
    {
        (ServiceProvider services, CapturingPublisher publisher) = await CreateAppAsync();
        await using ServiceProvider _ = services;

        string widgetId = Guid.NewGuid().ToString("n");

        await using (AsyncServiceScope scope = services.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            db.Widgets.Add(new WidgetRecord { Id = widgetId, Count = 3 });
            db.AddToOutbox("widgets", new WidgetCreated(widgetId, 3), o => o.MessageId = widgetId, _fixture.Clock);

            await db.SaveChangesAsync(Ct);
        }

        publisher.Published.ShouldBeEmpty("nothing is sent until the publisher runs");

        await CreatePublisher(services).PublishPendingAsync(Ct);

        publisher.Published.Count.ShouldBe(1);
        publisher.Published[0].Topic.ShouldBe("widgets");
        publisher.Published[0].Message.MessageId.ShouldBe(
            widgetId,
            "the staged message id is carried through so a retried publish can deduplicate");
    }

    [Fact]
    public async Task A_rolled_back_transaction_publishes_nothing()
    {
        // This is the whole point of the outbox: the data change and the announcement share a
        // fate. Without it, a crash between the two leaves them inconsistent in one direction or
        // the other, depending on the ordering you chose.
        (ServiceProvider services, CapturingPublisher publisher) = await CreateAppAsync();
        await using ServiceProvider _ = services;

        string widgetId = Guid.NewGuid().ToString("n");

        await using (AsyncServiceScope scope = services.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            await using Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction transaction =
                await db.Database.BeginTransactionAsync(Ct);

            db.Widgets.Add(new WidgetRecord { Id = widgetId, Count = 1 });
            db.AddToOutbox("widgets", new WidgetCreated(widgetId, 1), o => o.MessageId = widgetId, _fixture.Clock);

            await db.SaveChangesAsync(Ct);
            await transaction.RollbackAsync(Ct);
        }

        await CreatePublisher(services).PublishPendingAsync(Ct);

        publisher.Published.ShouldBeEmpty();

        await using AsyncServiceScope verify = services.CreateAsyncScope();
        AppDbContext check = verify.ServiceProvider.GetRequiredService<AppDbContext>();

        (await check.Widgets.AnyAsync(w => w.Id == widgetId, Ct))
            .ShouldBeFalse("neither the record nor its announcement survived");
    }

    [Fact]
    public async Task A_failed_publish_is_retried_with_backoff_and_eventually_gives_up()
    {
        (ServiceProvider services, CapturingPublisher publisher) = await CreateAppAsync();
        await using ServiceProvider _ = services;

        publisher.FailEveryPublish = true;

        string widgetId = Guid.NewGuid().ToString("n");

        await using (AsyncServiceScope scope = services.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            db.AddToOutbox("widgets", new WidgetCreated(widgetId, 1), o => o.MessageId = widgetId, _fixture.Clock);
            await db.SaveChangesAsync(Ct);
        }

        OutboxPublisher<AppDbContext> outbox = CreatePublisher(services);
        OutboxOptions options = services.GetRequiredService<IOptions<OutboxOptions>>().Value;

        for (int attempt = 0; attempt < options.MaxAttempts; attempt++)
        {
            await outbox.PublishPendingAsync(Ct);

            // Backoff means the row is not eligible again until time passes; without advancing the
            // clock the next pass would find nothing, which is exactly the intended behaviour.
            _fixture.Clock.Advance(options.MaxRetryDelay);
        }

        await using AsyncServiceScope verify = services.CreateAsyncScope();
        AppDbContext db2 = verify.ServiceProvider.GetRequiredService<AppDbContext>();

        OutboxMessage? message = await db2.Set<OutboxMessage>()
            .FirstOrDefaultAsync(m => m.MessageId == widgetId, Ct);

        message.ShouldNotBeNull();
        message.Status.ShouldBe(
            OutboxStatus.Failed,
            "one unpublishable message must not consume the publisher forever");
        message.LastError.ShouldNotBeNullOrEmpty("the operator needs to know why");
    }

    [Fact]
    public async Task A_recovered_broker_drains_the_backlog()
    {
        (ServiceProvider services, CapturingPublisher publisher) = await CreateAppAsync();
        await using ServiceProvider _ = services;

        publisher.FailEveryPublish = true;

        await using (AsyncServiceScope scope = services.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            for (int i = 0; i < 5; i++)
            {
                db.AddToOutbox(
                    "widgets",
                    new WidgetCreated($"w{i}", i),
                    o => o.MessageId = Guid.NewGuid().ToString("n"),
                    _fixture.Clock);
            }

            await db.SaveChangesAsync(Ct);
        }

        OutboxPublisher<AppDbContext> outbox = CreatePublisher(services);

        await outbox.PublishPendingAsync(Ct);
        publisher.Published.ShouldBeEmpty();

        publisher.FailEveryPublish = false;
        _fixture.Clock.Advance(TimeSpan.FromMinutes(10));

        await outbox.PublishPendingAsync(Ct);

        publisher.Published.Count.ShouldBe(5, "the backlog survived the outage and drained after it");
    }

    [Fact]
    public async Task Messages_are_published_in_the_order_they_were_staged()
    {
        (ServiceProvider services, CapturingPublisher publisher) = await CreateAppAsync();
        await using ServiceProvider _ = services;

        string prefix = Guid.NewGuid().ToString("n")[..8];

        await using (AsyncServiceScope scope = services.CreateAsyncScope())
        {
            AppDbContext db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            for (int i = 0; i < 5; i++)
            {
                db.AddToOutbox(
                    "widgets",
                    new WidgetCreated($"{prefix}-{i}", i),
                    o => o.MessageId = $"{prefix}-{i}",
                    _fixture.Clock);
            }

            await db.SaveChangesAsync(Ct);
        }

        await CreatePublisher(services).PublishPendingAsync(Ct);

        List<string> order = [.. publisher.Published
            .Select(p => p.Message.MessageId)
            .Where(id => id.StartsWith(prefix, StringComparison.Ordinal))];

        order.ShouldBe([$"{prefix}-0", $"{prefix}-1", $"{prefix}-2", $"{prefix}-3", $"{prefix}-4"]);
    }
}
