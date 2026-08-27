using Microsoft.EntityFrameworkCore;
using PubSub.Outbox;

namespace Sample.Shipping.Worker;

/// <summary>The event this worker consumes, mirroring the publisher's contract.</summary>
public sealed record OrderPlaced(
    string OrderId,
    string CustomerId,
    string Region,
    decimal Total,
    DateTimeOffset PlacedAt);

/// <summary>A shipment created in response to an order.</summary>
public sealed class Shipment
{
    /// <summary>The shipment's identifier.</summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>The order it fulfils.</summary>
    public string OrderId { get; set; } = string.Empty;

    /// <summary>The customer receiving it.</summary>
    public string CustomerId { get; set; } = string.Empty;

    /// <summary>Where it is going.</summary>
    public string Region { get; set; } = string.Empty;

    /// <summary>When it was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>The shipping service's database, including the inbox.</summary>
public sealed class ShippingDbContext : DbContext
{
    /// <summary>Creates the context.</summary>
    public ShippingDbContext(DbContextOptions<ShippingDbContext> options)
        : base(options)
    {
    }

    /// <summary>The shipments.</summary>
    public DbSet<Shipment> Shipments => Set<Shipment>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Shipment>(entity =>
        {
            entity.ToTable("Shipments");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasMaxLength(64);
            entity.Property(e => e.OrderId).HasMaxLength(64).IsRequired();
            entity.Property(e => e.CustomerId).HasMaxLength(64).IsRequired();
            entity.Property(e => e.Region).HasMaxLength(32).IsRequired();

            // One shipment per order, enforced by the database rather than by a prior read.
            entity.HasIndex(e => e.OrderId).IsUnique();
        });

        // The inbox lives here so a processed-message marker commits with the shipment it
        // accompanies.
        modelBuilder.AddPubSubOutbox();
    }
}
