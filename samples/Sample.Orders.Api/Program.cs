using Microsoft.EntityFrameworkCore;
using PubSub.Client;
using PubSub.Outbox;
using Sample.Orders.Api;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

string connectionString = builder.Configuration.GetConnectionString("Orders")
                          ?? throw new InvalidOperationException("No 'Orders' connection string is configured.");

builder.Services.AddDbContext<OrdersDbContext>(options => options.UseSqlServer(connectionString));

builder.Services.AddPubSubClient(builder.Configuration);
builder.Services.AddPubSubOutboxOptions(builder.Configuration);
builder.Services.AddPubSubOutbox<OrdersDbContext>();

builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.AddHealthChecks().AddDbContextCheck<OrdersDbContext>("orders-database");

WebApplication app = builder.Build();

using (IServiceScope scope = app.Services.CreateScope())
{
    OrdersDbContext db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.MigrateAsync();
}

app.MapOpenApi();
app.MapHealthChecks("/health/ready");

const string Topic = "orders";

app.MapPost("/orders", async (PlaceOrderRequest request, OrdersDbContext db, CancellationToken ct) =>
{
    Order order = new()
    {
        Id = request.OrderId ?? Guid.NewGuid().ToString("n"),
        CustomerId = request.CustomerId,
        Region = request.Region,
        Total = request.Total,
        PlacedAt = DateTimeOffset.UtcNow,
    };

    db.Orders.Add(order);

    // The order row and the publish intent are staged together, so the commit below is the single
    // point at which both become real. There is no window where one exists without the other.
    db.AddToOutbox(
        Topic,
        new OrderPlaced(order.Id, order.CustomerId, order.Region, order.Total, order.PlacedAt),
        options =>
        {
            // Keying the message on the order id lets duplicate detection recognise a retried
            // publish as the same message rather than a second order.
            options.MessageId = order.Id;
            options.CorrelationId = order.Id;

            // Per-customer ordering: two orders from one customer are processed in sequence,
            // while different customers proceed concurrently.
            options.SessionId = order.CustomerId;

            // Routing metadata, kept scalar so subscription filters can match on it cheaply.
            options.ApplicationProperties["region"] = order.Region;
            options.ApplicationProperties["total"] = order.Total;
        });

    await db.SaveChangesAsync(ct);

    return TypedResults.Created($"/orders/{order.Id}", new { order.Id });
})
.WithName("PlaceOrder")
.WithSummary("Places an order, staging its event in the same transaction.");

app.MapPost("/orders/{id}/remind", async (
    string id,
    int afterMinutes,
    OrdersDbContext db,
    CancellationToken ct) =>
{
    Order? order = await db.Orders.FindAsync([id], ct);
    if (order is null)
    {
        return Results.NotFound();
    }

    // Scheduled publishing: the message is stored now but stays invisible to consumers until its
    // time, which is how a deferred follow-up is expressed without a timer in the application.
    db.AddToOutbox(
        Topic,
        new OrderPlaced(order.Id, order.CustomerId, order.Region, order.Total, order.PlacedAt),
        options =>
        {
            options.MessageId = $"{order.Id}-reminder";
            options.Subject = "OrderReminder";
            options.ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddMinutes(afterMinutes);
            options.ApplicationProperties["region"] = order.Region;
        });

    await db.SaveChangesAsync(ct);

    return Results.Accepted();
})
.WithName("ScheduleOrderReminder")
.WithSummary("Schedules a reminder for an order, published only when its time arrives.");

app.MapGet("/orders/{id}", async (string id, OrdersDbContext db, CancellationToken ct) =>
    await db.Orders.FindAsync([id], ct) is { } order
        ? Results.Ok(order)
        : Results.NotFound())
.WithName("GetOrder");

await app.RunAsync();

/// <summary>A request to place an order.</summary>
/// <param name="CustomerId">The customer placing it; also the session key.</param>
/// <param name="Region">Where it is going.</param>
/// <param name="Total">The order total.</param>
/// <param name="OrderId">An explicit id, if the caller wants their retry to deduplicate.</param>
internal sealed record PlaceOrderRequest(
    string CustomerId,
    string Region,
    decimal Total,
    string? OrderId = null);

/// <summary>Exposes the entry point to tests.</summary>
public partial class Program;
