using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PubSub.Abstractions;

namespace Sample.Shipping.Worker;

/// <summary>
/// Creates a shipment for an order.
/// </summary>
/// <remarks>
/// Written to be naturally idempotent: the shipment id is derived from the order id and the write
/// is an upsert, so a redelivered message produces the same row rather than a second shipment.
/// That is cheaper and more robust than deduplication bookkeeping, and it is the first thing to
/// reach for when a handler's work allows it.
/// </remarks>
public sealed class CreateShipmentHandler : IMessageHandler<OrderPlaced>
{
    private readonly ShippingDbContext _db;
    private readonly ILogger<CreateShipmentHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public CreateShipmentHandler(ShippingDbContext db, ILogger<CreateShipmentHandler> logger)
    {
        _db = db;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleAsync(
        MessageContext<OrderPlaced> context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        OrderPlaced order = context.Payload;
        string shipmentId = $"shp-{order.OrderId}";

        Shipment? existing = await _db.Shipments
            .FirstOrDefaultAsync(s => s.OrderId == order.OrderId, cancellationToken);

        if (existing is not null)
        {
            SampleLog.ShipmentAlreadyExists(_logger, order.OrderId, context.DeliveryCount);
            return;
        }

        _db.Shipments.Add(new Shipment
        {
            Id = shipmentId,
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            Region = order.Region,
            CreatedAt = DateTimeOffset.UtcNow,
        });

        await _db.SaveChangesAsync(cancellationToken);

        SampleLog.ShipmentCreated(_logger, shipmentId, order.OrderId, order.Region);
    }
}

/// <summary>
/// Handles high-value orders, demonstrating a filtered subscription.
/// </summary>
/// <remarks>
/// This handler never sees a low-value order, because the subscription's rule excludes them at the
/// broker. Filtering there rather than here means the message is never delivered at all, instead
/// of being delivered and discarded.
/// </remarks>
public sealed class HighValueOrderHandler : IMessageHandler<OrderPlaced>
{
    private readonly ILogger<HighValueOrderHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public HighValueOrderHandler(ILogger<HighValueOrderHandler> logger) => _logger = logger;

    /// <inheritdoc />
    public Task HandleAsync(
        MessageContext<OrderPlaced> context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        SampleLog.HighValueOrder(_logger, context.Payload.OrderId, context.Payload.Total);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Rejects orders it cannot process, demonstrating the two failure paths.
/// </summary>
/// <remarks>
/// A transient failure throws, so the message is redelivered until the subscription's budget runs
/// out. A permanent one dead-letters immediately, because retrying a message that can never
/// succeed only spends attempts and delays the alert.
/// </remarks>
public sealed class ValidatingOrderHandler : IMessageHandler<OrderPlaced>
{
    private readonly ILogger<ValidatingOrderHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public ValidatingOrderHandler(ILogger<ValidatingOrderHandler> logger) => _logger = logger;

    /// <inheritdoc />
    public async Task HandleAsync(
        MessageContext<OrderPlaced> context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Payload.Total < 0)
        {
            // No number of retries will make a negative total valid.
            await context.DeadLetterAsync(
                DeadLetterReason.ApplicationError,
                $"Order '{context.Payload.OrderId}' has a negative total.",
                cancellationToken);

            return;
        }

        if (string.IsNullOrWhiteSpace(context.Payload.Region))
        {
            throw new InvalidOperationException(
                $"Order '{context.Payload.OrderId}' has no region; retrying in case it is enriched later.");
        }

        SampleLog.OrderValidated(_logger, context.Payload.OrderId);
    }
}

/// <summary>
/// Processes a customer's orders in sequence, demonstrating sessions.
/// </summary>
/// <remarks>
/// Messages sharing a session id reach this handler one at a time, in publish order. Different
/// customers are processed concurrently, which is what keeps ordering from costing global
/// throughput.
/// </remarks>
public sealed class CustomerTimelineHandler : IMessageHandler<OrderPlaced>
{
    private readonly ILogger<CustomerTimelineHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public CustomerTimelineHandler(ILogger<CustomerTimelineHandler> logger) => _logger = logger;

    /// <inheritdoc />
    public Task HandleAsync(
        MessageContext<OrderPlaced> context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        SampleLog.CustomerOrderProcessed(
            _logger,
            context.SessionId ?? "(none)",
            context.Payload.OrderId,
            context.Envelope.SequenceNumber);

        return Task.CompletedTask;
    }
}

internal static partial class SampleLog
{
    [LoggerMessage(
        EventId = 4000,
        Level = LogLevel.Information,
        Message = "Created shipment '{ShipmentId}' for order '{OrderId}' to {Region}.")]
    public static partial void ShipmentCreated(
        ILogger logger, string shipmentId, string orderId, string region);

    [LoggerMessage(
        EventId = 4001,
        Level = LogLevel.Information,
        Message = "Order '{OrderId}' already has a shipment; delivery {DeliveryCount} is a repeat.")]
    public static partial void ShipmentAlreadyExists(ILogger logger, string orderId, int deliveryCount);

    [LoggerMessage(
        EventId = 4002,
        Level = LogLevel.Information,
        Message = "High-value order '{OrderId}' totalling {Total}.")]
    public static partial void HighValueOrder(ILogger logger, string orderId, decimal total);

    [LoggerMessage(
        EventId = 4003,
        Level = LogLevel.Information,
        Message = "Order '{OrderId}' validated.")]
    public static partial void OrderValidated(ILogger logger, string orderId);

    [LoggerMessage(
        EventId = 4004,
        Level = LogLevel.Information,
        Message = "Session '{SessionId}': order '{OrderId}' at sequence {SequenceNumber}.")]
    public static partial void CustomerOrderProcessed(
        ILogger logger, string sessionId, string orderId, long sequenceNumber);
}
