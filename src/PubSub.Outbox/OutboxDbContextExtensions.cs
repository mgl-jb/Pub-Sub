using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;

namespace PubSub.Outbox;

/// <summary>Enlists publish intents in the caller's own transaction.</summary>
public static class OutboxDbContextExtensions
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Adds a message to the outbox so it commits with the surrounding data change.
    /// </summary>
    /// <remarks>
    /// This only stages the intent. Nothing is sent until the caller's <c>SaveChangesAsync</c>
    /// commits — which is the whole point: a rolled-back transaction publishes nothing.
    /// </remarks>
    public static OutboxMessage AddToOutbox<T>(
        this DbContext context,
        string topic,
        T payload,
        Action<PublishOptions>? configure = null,
        TimeProvider? timeProvider = null)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(payload);

        PublishOptions options = new();
        configure?.Invoke(options);

        DateTimeOffset now = (timeProvider ?? TimeProvider.System).GetUtcNow();

        OutboxMessage message = new()
        {
            Topic = topic,
            MessageId = options.MessageId ?? Guid.NewGuid().ToString("n"),
            Subject = options.Subject ?? typeof(T).Name,
            CorrelationId = options.CorrelationId,
            SessionId = options.SessionId,
            ContentType = "application/json",
            Body = JsonSerializer.SerializeToUtf8Bytes(payload, Json),
            ApplicationPropertiesJson = options.ApplicationProperties.Count == 0
                ? null
                : JsonSerializer.Serialize(options.ApplicationProperties, Json),
            ScheduledEnqueueTime = options.ScheduledEnqueueTime,
            Status = OutboxStatus.Pending,
            NextAttemptAt = now,
            CreatedAt = now,
        };

        context.Set<OutboxMessage>().Add(message);
        return message;
    }

    /// <summary>Adds the outbox and inbox tables to a model.</summary>
    /// <remarks>Call from the application's <c>OnModelCreating</c>.</remarks>
    public static ModelBuilder AddPubSubOutbox(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("OutboxMessages");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Topic).HasMaxLength(260).IsRequired();
            entity.Property(e => e.MessageId).HasMaxLength(260).IsRequired();
            entity.Property(e => e.Subject).HasMaxLength(260);
            entity.Property(e => e.CorrelationId).HasMaxLength(260);
            entity.Property(e => e.SessionId).HasMaxLength(128);
            entity.Property(e => e.ContentType).HasMaxLength(260).IsRequired();
            entity.Property(e => e.ClaimedBy).HasMaxLength(128);
            entity.Property(e => e.LastError).HasMaxLength(2048);

            // The dispatch query filters on status and readiness and orders by Id, so this index
            // lets the ordered TOP(n) claim stop early instead of sorting the whole backlog.
            entity.HasIndex(e => new { e.Status, e.NextAttemptAt, e.Id })
                .HasDatabaseName("IX_OutboxMessages_Dispatch");

            // Supports pruning published rows.
            entity.HasIndex(e => new { e.Status, e.PublishedAt })
                .HasDatabaseName("IX_OutboxMessages_Published");
        });

        modelBuilder.Entity<InboxMessage>(entity =>
        {
            entity.ToTable("InboxMessages");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.MessageId).HasMaxLength(260).IsRequired();
            entity.Property(e => e.Consumer).HasMaxLength(260).IsRequired();

            // Deduplication is enforced by this constraint rather than by a prior read: checking
            // first would leave a window in which two concurrent deliveries both pass the check.
            entity.HasIndex(e => new { e.MessageId, e.Consumer })
                .IsUnique()
                .HasDatabaseName("UX_InboxMessages_Processed");

            entity.HasIndex(e => e.ExpiresAt).HasDatabaseName("IX_InboxMessages_Expiry");
        });

        return modelBuilder;
    }
}
