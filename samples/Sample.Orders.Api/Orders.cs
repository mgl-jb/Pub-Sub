using Microsoft.EntityFrameworkCore;
using PubSub.Outbox;

namespace Sample.Orders.Api;

/// <summary>An order, as this service stores it.</summary>
public sealed class Order
{
    /// <summary>The order's identifier, also used as the message id so retries deduplicate.</summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>The customer who placed it; doubles as the session key for ordered processing.</summary>
    public string CustomerId { get; set; } = string.Empty;

    /// <summary>Where the order is going, used by region-filtered subscriptions.</summary>
    public string Region { get; set; } = string.Empty;

    /// <summary>The order total, used by value-filtered subscriptions.</summary>
    public decimal Total { get; set; }

    /// <summary>When the order was placed.</summary>
    public DateTimeOffset PlacedAt { get; set; }
}

/// <summary>The event published when an order is placed.</summary>
/// <remarks>
/// Deliberately carries the resulting state rather than a delta. A consumer applying an absolute
/// value is naturally idempotent, so a redelivery changes nothing — which is a far cheaper way to
/// survive at-least-once delivery than deduplication bookkeeping.
/// </remarks>
public sealed record OrderPlaced(
    string OrderId,
    string CustomerId,
    string Region,
    decimal Total,
    DateTimeOffset PlacedAt);

/// <summary>The orders service's database, including the outbox.</summary>
public sealed class OrdersDbContext : DbContext
{
    /// <summary>Creates the context.</summary>
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options)
        : base(options)
    {
    }

    /// <summary>The orders.</summary>
    public DbSet<Order> Orders => Set<Order>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasMaxLength(64);
            entity.Property(e => e.CustomerId).HasMaxLength(64).IsRequired();
            entity.Property(e => e.Region).HasMaxLength(32).IsRequired();
            entity.Property(e => e.Total).HasPrecision(18, 2);
        });

        // The outbox lives in this database on purpose: that is what lets an order and the intent
        // to announce it commit in one transaction.
        modelBuilder.AddPubSubOutbox();
    }
}
