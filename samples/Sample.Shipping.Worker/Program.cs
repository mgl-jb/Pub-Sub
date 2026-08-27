using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PubSub.Client;
using PubSub.Outbox;
using Sample.Shipping.Worker;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

string connectionString = builder.Configuration.GetConnectionString("Shipping")
                          ?? throw new InvalidOperationException("No 'Shipping' connection string is configured.");

builder.Services.AddDbContext<ShippingDbContext>(options => options.UseSqlServer(connectionString));

builder.Services.AddPubSubClient(builder.Configuration);
builder.Services.AddPubSubOutboxOptions(builder.Configuration);
builder.Services.AddInboxCleanup<ShippingDbContext>();

const string Topic = "orders";

// Handlers are declared on the processor that dispatches them, because all three subscriptions
// below carry the same subject and each needs a different handler for it.

builder.Services.AddMessageProcessor(options =>
{
    options.Topic = Topic;
    options.Subscription = "shipping";

    // Naturally idempotent: the shipment is keyed on the order id and written as an upsert, so a
    // redelivery produces the same row rather than a second shipment. No inbox needed.
    options.Handlers.Add<OrderPlaced, CreateShipmentHandler>("OrderPlaced");

    // Several orders at once, since shipments are independent of each other.
    options.MaxConcurrentCalls = 4;
    options.PrefetchCount = 8;
});

// A filtered subscription: the broker only delivers high-value orders here, so this consumer
// never spends time receiving and discarding the rest.
builder.Services.AddMessageProcessor(options =>
{
    options.Topic = Topic;
    options.Subscription = "high-value";
    options.Handlers.Add<OrderPlaced, HighValueOrderHandler>("OrderPlaced");
    options.MaxConcurrentCalls = 2;
});

// A validating consumer, demonstrating both failure paths: throwing for a transient problem so
// the message is retried, and dead-lettering immediately for one that never will succeed.
builder.Services.AddMessageProcessor(options =>
{
    options.Topic = Topic;
    options.Subscription = "validation";
    options.Handlers.Add<OrderPlaced, ValidatingOrderHandler>("OrderPlaced");
});

// A session processor: one customer's orders are handled strictly in order, while different
// customers proceed concurrently.
builder.Services.AddSessionProcessor(options =>
{
    options.Topic = Topic;
    options.Subscription = "customer-timeline";
    options.Handlers.Add<OrderPlaced, CustomerTimelineHandler>("OrderPlaced");
    options.MaxConcurrentSessions = 4;
});

// Ensure the entities this worker consumes from exist before it starts pulling.
builder.Services.AddHostedService<TopologyProvisioner>();

IHost host = builder.Build();

using (IServiceScope scope = host.Services.CreateScope())
{
    ShippingDbContext db = scope.ServiceProvider.GetRequiredService<ShippingDbContext>();
    await db.Database.MigrateAsync();
}

await host.RunAsync();
