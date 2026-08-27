# Client library

`PubSub.Client` is what applications use. Handlers deal with payloads; the receive loop,
concurrency, lock renewal, settlement, and trace propagation are handled for them.

## Setup

```csharp
builder.Services.AddPubSubClient(builder.Configuration);   // binds the "PubSub" section
```

```json
{
  "PubSub": {
    "BrokerUri": "https://broker.example.com",
    "ReceiverId": "orders-worker",
    "RequestTimeout": "00:00:30"
  }
}
```

`ReceiverId` appears in the broker's lock diagnostics; it defaults to machine name and process id,
which is usually enough to identify which instance holds a message.

## Publishing

```csharp
public class PlaceOrder(IEventPublisher publisher)
{
    public Task PublishAsync(Order order) =>
        publisher.PublishAsync("orders", new OrderPlaced(order.Id, order.Total), options =>
        {
            options.MessageId = order.Id;          // a retried send deduplicates
            options.SessionId = order.CustomerId;  // per-customer ordering
            options.ApplicationProperties["region"] = order.Region;
        });
}
```

Prefer `PublishBatchAsync` for several messages: the batch is atomic and costs one round trip.

For a message that must not be published unless a database change commits, use the outbox rather
than this — see [`reliability.md`](reliability.md).

## Consuming

```csharp
public sealed class ShipOrderHandler(ShippingDbContext db) : IMessageHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken ct)
    {
        // Delivery is at-least-once, so this must tolerate being called twice for one message.
        await db.Shipments.Upsert(context.Payload.OrderId, ...);
        await db.SaveChangesAsync(ct);
    }
}
```

```csharp
builder.Services.AddMessageProcessor(options =>
{
    options.Topic = "orders";
    options.Subscription = "shipping";
    options.Handlers.Add<OrderPlaced, ShipOrderHandler>("OrderPlaced");
    options.MaxConcurrentCalls = 4;
    options.PrefetchCount = 8;
});
```

Handlers belong to the **processor**, not the process. One worker commonly consumes several
subscriptions of the same topic and needs a different handler for each — a shipping subscription
and a high-value subscription both carry `OrderPlaced`, and a single shared registry could route
only one of them.

Each message runs in its own dependency-injection scope, so a handler can depend on scoped services
exactly as an HTTP request handler would.

### What happens automatically

- Returning normally **completes** the message.
- Throwing **abandons** it, so it is redelivered until the budget runs out.
- The lock is **renewed** while the handler works, up to `MaxAutoLockRenewalDuration`.
- A payload that cannot be deserialized is **dead-lettered**, not retried — it will not parse next
  time either.
- A subject with no registered handler is **dead-lettered**, since the handler will still be
  missing on the next attempt.

### Overriding it

```csharp
if (context.Payload.Total < 0)
{
    // No number of retries makes this valid.
    await context.DeadLetterAsync(DeadLetterReason.ApplicationError, "Negative total.", ct);
    return;
}
```

A handler that settles explicitly claims the right to do so, so auto-complete does not fire again
and fail with a lost lock.

## Sessions

```csharp
builder.Services.AddSessionProcessor(options =>
{
    options.Topic = "orders";
    options.Subscription = "customer-timeline";
    options.Handlers.Add<OrderPlaced, TimelineHandler>("OrderPlaced");
    options.MaxConcurrentSessions = 4;
});
```

Concurrency is across sessions, never within one. `MaxConcurrentSessions` is how many customers are
processed at once; each customer's messages are still handled one at a time, in order.

Sessions are released after `SessionIdleTimeout`, which stops one worker accumulating locks on
sessions that have gone quiet.

## The options that matter

| Option | Effect | Getting it wrong |
| --- | --- | --- |
| `MaxConcurrentCalls` | Messages in flight at once | Too high and locks expire during processing |
| `PrefetchCount` | Messages claimed per receive | Too high and prefetched messages expire unhandled |
| `MaxWaitTime` | Long-poll duration | Too short and receives become a polling loop |
| `MaxAutoLockRenewalDuration` | Renewal ceiling | Too low and long work loses its lock |
| `AutoComplete` | Settle on return | Off means the handler must settle on every path |
| `ErrorBackoff` | Pause after a receive failure | Too short and a broker outage becomes a stampede |

Concurrency and prefetch interact: every in-flight and prefetched message holds a lock, so setting
either far above what handlers keep up with produces redeliveries rather than throughput.

## Tracing

The client injects W3C trace context into message properties on publish and extracts it on
consume, so a publish and the consume it caused form **one trace** rather than two unrelated ones.

Register the sources with OpenTelemetry:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(PubSubDiagnostics.ActivitySourceName))
    .WithMetrics(m => m.AddMeter(PubSubDiagnostics.MeterName));
```

| Instrument | Records |
| --- | --- |
| `pubsub.client.published` | Messages accepted by the broker |
| `pubsub.client.received` | Messages handed to a handler |
| `pubsub.client.settled` | Settlements, tagged by kind |
| `pubsub.client.handler_failures` | Handler exceptions, tagged by type |
| `pubsub.client.handler.duration` | Time in the handler |

Handler duration approaching the lock duration is the most useful early warning: it precedes lock
expiry, which precedes duplicate processing.

## Shutdown

`StopAsync` stops receiving and waits for in-flight handlers to finish. Draining rather than
abandoning matters — a handler cut off mid-message means that message is redelivered, which turns
"some work repeats on every deploy" into "none does".

## Exceptions

| Exception | Meaning |
| --- | --- |
| `MessageLockLostException` | The lock expired; the message may already be redelivered |
| `SessionLockLostException` | The session lock was lost; its messages return to the subscription |
| `EntityNotFoundException` | No such topic, subscription, or delivery |
| `InvalidOperationForStateException` | The operation is invalid for the entity's state |
| `BrokerUnavailableException` | Transient; `IsTransient` is true and the pipeline retries |

Only `BrokerUnavailableException` reports `IsTransient`. A lost lock is not retryable — the message
belongs to someone else now.
