using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

/// <summary>The broker's durable store.</summary>
/// <remarks>
/// SQL is the system of record for everything: entities, messages, delivery state, and locks.
/// Redis, where configured, only accelerates dispatch and caches compiled rules — it holds no
/// state the broker cannot rebuild from here.
/// </remarks>
public sealed class BrokerDbContext : DbContext
{
    /// <summary>Creates the context.</summary>
    public BrokerDbContext(DbContextOptions<BrokerDbContext> options)
        : base(options)
    {
    }

    /// <summary>Topics.</summary>
    public DbSet<TopicEntity> Topics => Set<TopicEntity>();

    /// <summary>Subscriptions.</summary>
    public DbSet<SubscriptionEntity> Subscriptions => Set<SubscriptionEntity>();

    /// <summary>Subscription rules.</summary>
    public DbSet<RuleEntity> Rules => Set<RuleEntity>();

    /// <summary>Published messages, stored once per topic.</summary>
    public DbSet<MessageEntity> Messages => Set<MessageEntity>();

    /// <summary>Per-subscription deliveries — the unit consumers claim and settle.</summary>
    public DbSet<DeliveryEntity> Deliveries => Set<DeliveryEntity>();

    /// <summary>Session locks granting exclusive ordered access.</summary>
    public DbSet<SessionLockEntity> SessionLocks => Set<SessionLockEntity>();

    /// <summary>Duplicate detection records.</summary>
    public DbSet<DedupEntity> DedupEntries => Set<DedupEntity>();

    /// <summary>
    /// Stores every <see cref="TimeSpan"/> as ticks rather than as a SQL <c>time</c>.
    /// </summary>
    /// <remarks>
    /// SQL Server's <c>time</c> type represents a time of day, so it caps at just under 24 hours.
    /// The broker's durations routinely exceed that — a default time to live of 14 days is
    /// ordinary — and would otherwise overflow on insert. Ticks in a <c>bigint</c> round-trip
    /// exactly and have no such ceiling.
    /// </remarks>
    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        ArgumentNullException.ThrowIfNull(configurationBuilder);
        base.ConfigureConventions(configurationBuilder);

        configurationBuilder.Properties<TimeSpan>()
            .HaveConversion<Microsoft.EntityFrameworkCore.Storage.ValueConversion.TimeSpanToTicksConverter>();
    }

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        base.OnModelCreating(modelBuilder);

        ConfigureTopics(modelBuilder);
        ConfigureSubscriptions(modelBuilder);
        ConfigureRules(modelBuilder);
        ConfigureMessages(modelBuilder);
        ConfigureDeliveries(modelBuilder);
        ConfigureSessionLocks(modelBuilder);
        ConfigureDedup(modelBuilder);
    }

    private static void ConfigureTopics(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<TopicEntity>(entity =>
        {
            entity.ToTable("Topics");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(260).IsRequired();
            entity.HasIndex(e => e.Name).IsUnique();
        });

    private static void ConfigureSubscriptions(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<SubscriptionEntity>(entity =>
        {
            entity.ToTable("Subscriptions");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(260).IsRequired();
            entity.HasIndex(e => new { e.TopicId, e.Name }).IsUnique();

            entity.HasOne(e => e.Topic)
                .WithMany(t => t.Subscriptions)
                .HasForeignKey(e => e.TopicId)
                .OnDelete(DeleteBehavior.Cascade);
        });

    private static void ConfigureRules(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<RuleEntity>(entity =>
        {
            entity.ToTable("Rules");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(260).IsRequired();
            entity.Property(e => e.SqlExpression).HasMaxLength(4096);
            entity.Property(e => e.ActionExpression).HasMaxLength(4096);
            entity.HasIndex(e => new { e.SubscriptionId, e.Name }).IsUnique();

            entity.HasOne(e => e.Subscription)
                .WithMany(s => s.Rules)
                .HasForeignKey(e => e.SubscriptionId)
                .OnDelete(DeleteBehavior.Cascade);
        });

    private static void ConfigureMessages(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<MessageEntity>(entity =>
        {
            entity.ToTable("Messages");
            entity.HasKey(e => e.SequenceNumber);

            // An identity column gives monotonically increasing sequence numbers in publish
            // order, which is what session ordering and deferred retrieval both rely on.
            entity.Property(e => e.SequenceNumber).ValueGeneratedOnAdd();

            entity.Property(e => e.MessageId).HasMaxLength(260).IsRequired();
            entity.Property(e => e.CorrelationId).HasMaxLength(260);
            entity.Property(e => e.Subject).HasMaxLength(260);
            entity.Property(e => e.ContentType).HasMaxLength(260).IsRequired();
            entity.Property(e => e.SessionId).HasMaxLength(128);
            entity.Property(e => e.ReplyTo).HasMaxLength(260);
            entity.Property(e => e.ReplyToSessionId).HasMaxLength(128);
            entity.Property(e => e.To).HasMaxLength(260);

            entity.HasOne(e => e.Topic)
                .WithMany()
                .HasForeignKey(e => e.TopicId)
                .OnDelete(DeleteBehavior.Cascade);

            // Supports pruning messages whose deliveries are all settled and whose TTL has passed.
            entity.HasIndex(e => new { e.TopicId, e.ExpiresAt });
        });

    private static void ConfigureDeliveries(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<DeliveryEntity>(entity =>
        {
            entity.ToTable("Deliveries");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.SessionId).HasMaxLength(128);
            entity.Property(e => e.LockedBy).HasMaxLength(128);
            entity.Property(e => e.DeadLetterReason).HasMaxLength(128);
            entity.Property(e => e.DeadLetterDescription).HasMaxLength(2048);

            entity.HasOne(e => e.Message)
                .WithMany(m => m.Deliveries)
                .HasForeignKey(e => e.MessageSequenceNumber)
                .OnDelete(DeleteBehavior.Cascade);

            // Deliveries would otherwise be reachable from Topics by two cascade paths — through
            // Messages and through Subscriptions — which SQL Server rejects outright. The message
            // path keeps its cascade because pruning relies on it constantly; deleting a
            // subscription removes its deliveries explicitly instead, which is a rare admin
            // operation and cheap to do by hand.
            entity.HasOne(e => e.Subscription)
                .WithMany()
                .HasForeignKey(e => e.SubscriptionId)
                .OnDelete(DeleteBehavior.NoAction);

            // The claim query is the hottest in the system. Its predicate is
            // (SubscriptionId, State, AvailableAt) ordered by SequenceNumber, so this index lets
            // the ordered TOP(n) scan stop as soon as it has enough rows instead of sorting the
            // whole backlog.
            entity.HasIndex(e => new { e.SubscriptionId, e.State, e.AvailableAt, e.SequenceNumber })
                .HasDatabaseName("IX_Deliveries_Claim");

            // The same claim, narrowed to one session.
            entity.HasIndex(e => new { e.SubscriptionId, e.SessionId, e.State, e.SequenceNumber })
                .HasDatabaseName("IX_Deliveries_SessionClaim");

            // Lets the sweeper find expired locks without scanning live rows.
            entity.HasIndex(e => new { e.State, e.LockedUntil })
                .HasDatabaseName("IX_Deliveries_LockExpiry");

            // Supports TTL expiry sweeps.
            entity.HasIndex(e => new { e.State, e.ExpiresAt })
                .HasDatabaseName("IX_Deliveries_Expiry");

            // Supports dead-letter listing and pruning of settled rows.
            entity.HasIndex(e => new { e.SubscriptionId, e.State, e.SettledAt })
                .HasDatabaseName("IX_Deliveries_Settled");

            // Deferred messages are retrieved by sequence number within a subscription.
            entity.HasIndex(e => new { e.SubscriptionId, e.SequenceNumber })
                .HasDatabaseName("IX_Deliveries_Sequence");
        });

    private static void ConfigureSessionLocks(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<SessionLockEntity>(entity =>
        {
            entity.ToTable("SessionLocks");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.SessionId).HasMaxLength(128).IsRequired();
            entity.Property(e => e.LockedBy).HasMaxLength(128);

            entity.HasOne(e => e.Subscription)
                .WithMany()
                .HasForeignKey(e => e.SubscriptionId)
                .OnDelete(DeleteBehavior.Cascade);

            // Exclusivity is enforced by the database, not by application logic: two consumers
            // racing to accept the same session both attempt this insert, and exactly one wins.
            entity.HasIndex(e => new { e.SubscriptionId, e.SessionId })
                .IsUnique()
                .HasDatabaseName("UX_SessionLocks_Session");

            entity.HasIndex(e => e.LockedUntil).HasDatabaseName("IX_SessionLocks_Expiry");
        });

    private static void ConfigureDedup(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<DedupEntity>(entity =>
        {
            entity.ToTable("DedupEntries");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.MessageId).HasMaxLength(260).IsRequired();

            // Duplicate suppression is a unique-constraint violation rather than a read-then-write:
            // checking first would leave a window in which two concurrent publishes both pass the
            // check and both insert.
            entity.HasIndex(e => new { e.TopicId, e.MessageId })
                .IsUnique()
                .HasDatabaseName("UX_DedupEntries_MessageId");

            entity.HasIndex(e => e.ExpiresAt).HasDatabaseName("IX_DedupEntries_Expiry");
        });
}
