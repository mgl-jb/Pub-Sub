# PubSub

A publish/subscribe message broker in C# on .NET 10, deployed on Azure.

The broker is **built rather than bought**: Azure Service Bus, Event Grid, and Event Hubs are all
out of scope by requirement, so their capabilities are implemented here on Azure's non-messaging
primitives — SQL Database as the system of record, Cache for Redis to accelerate dispatch, Container
Apps to run it, and Entra ID with managed identity so no connection secret exists anywhere.
[ADR 1](docs/adr/0001-build-our-own-broker.md) covers the reasoning and the boundaries.

## What it does

| Capability | Notes |
| --- | --- |
| Durable topics and subscriptions | Payload stored once per topic, delivery state per subscription |
| Filtered routing | Correlation filters, plus a SQL-92-like expression language with three-valued logic |
| Peek-lock settlement | Complete, abandon, dead-letter, defer, renew |
| Competing consumers | `READPAST` claiming, so N consumers give N consumers' throughput |
| Retry and dead-lettering | Delivery budgets, delayed retry, browse and replay |
| Ordering | Sessions, with per-key FIFO and cross-key concurrency |
| Scheduled and deferred delivery | Publish for later; set aside and retrieve by sequence number |
| Duplicate detection | Suppresses a producer's retried send within a window |
| Transactional outbox and inbox | A data change and its announcement share a fate |
| Observability | OpenTelemetry traces spanning publish and consume, metrics, health |

Delivery is **at-least-once**, and consumers must be idempotent. That is not an implementation
detail to fix later — it is the contract, and [`docs/reliability.md`](docs/reliability.md) explains
how to satisfy it.

## Try it

```bash
cp deploy/.env.example deploy/.env      # change the password
docker compose -f deploy/docker-compose.yml up --build

curl -X POST http://localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","region":"emea","total":750}'

docker compose -f deploy/docker-compose.yml logs -f shipping-worker
```

The order appears twice in the worker logs, from two different handlers — that is fan-out to two
matching subscriptions, not a duplicate.

## Using it

Publish:

```csharp
await publisher.PublishAsync("orders", new OrderPlaced(id, total), options =>
{
    options.MessageId = order.Id;          // a retried send deduplicates
    options.SessionId = order.CustomerId;  // per-customer ordering
    options.ApplicationProperties["region"] = order.Region;
});
```

Consume by writing a handler:

```csharp
public sealed class ShipOrderHandler(ShippingDbContext db) : IMessageHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken ct)
    {
        // At-least-once: this must tolerate being called twice for one message.
        await db.Shipments.UpsertAsync(context.Payload.OrderId, ct);
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
});
```

Publish atomically with a database change:

```csharp
db.Orders.Add(order);
db.AddToOutbox("orders", new OrderPlaced(order.Id, order.Total));
await db.SaveChangesAsync();   // both, or neither
```

## Repository layout

```
src/
  PubSub.Abstractions     Envelope, publisher and handler contracts, filters, exceptions
  PubSub.Filters          Tokenizer, parser, and closure-compiled filter evaluator
  PubSub.Broker.Core      EF Core model, fan-out, peek-lock, sessions, sweeper
  PubSub.Broker.Redis     Optional wakeups and leader election
  PubSub.Broker.Api       HTTP surface, auth, OpenAPI, health, observability
  PubSub.Client           IEventPublisher, processor pump, lock renewal, tracing
  PubSub.Outbox           Transactional outbox and inbox deduplication
samples/                  Orders API, shipping worker, admin CLI
tests/                    Filter unit tests, broker integration tests, end-to-end tests
infra/                    Bicep templates and parameters
deploy/                   Dockerfiles and the compose stack
docs/                     Architecture, ADRs, and reference
```

## Documentation

| Document | Covers |
| --- | --- |
| [architecture.md](docs/architecture.md) | Components, fan-out, the peek-lock state machine, sessions |
| [reliability.md](docs/reliability.md) | The delivery guarantee, idempotency, retry, what is *not* guaranteed |
| [data-model.md](docs/data-model.md) | Tables, indexes, and why each exists |
| [filter-language.md](docs/filter-language.md) | Grammar, operators, three-valued logic |
| [api.md](docs/api.md) | REST contract and status-code semantics |
| [client-library.md](docs/client-library.md) | Handlers, processors, options and their failure modes |
| [observability.md](docs/observability.md) | Traces, metrics, what to alert on |
| [operations.md](docs/operations.md) | Deploying, scaling, tuning, incident runbooks |
| [local-development.md](docs/local-development.md) | Running and debugging locally |
| [adr/](docs/adr/) | Ten decision records, each with its rejected alternatives |
| [plan.md](docs/plan.md) | The implementation plan, kept as the record of intent |

## Building and testing

```bash
dotnet build
dotnet test --project tests/PubSub.Filters.Tests/PubSub.Filters.Tests.csproj
```

The integration and end-to-end suites start SQL Server through Testcontainers, so Docker must be
running:

```bash
dotnet test --project tests/PubSub.Broker.Tests/PubSub.Broker.Tests.csproj
dotnet test --project tests/PubSub.E2E.Tests/PubSub.E2E.Tests.csproj
```

They deliberately do **not** use an in-memory provider. The behaviour that matters here —
`READPAST` claiming, row locking, unique-constraint races — exists only in a real database engine,
so a suite using a fake would pass while proving nothing.

## Deploying to Azure

```bash
az deployment group create \
  --resource-group rg-pubsub-prod \
  --template-file infra/main.bicep \
  --parameters infra/main.prod.bicepparam
```

One step Bicep cannot perform: creating the SQL user for the managed identity. See
[operations.md](docs/operations.md#the-step-bicep-cannot-do).

## Limits worth knowing before adopting this

- **Throughput is bounded by one SQL database's write rate.** Sharding topics across databases is
  possible but not implemented.
- **Single region.** No geo-replication.
- **Ordering is per-session only.** There is no global ordering.
- **The claim query is SQL Server-specific.** PostgreSQL would use `FOR UPDATE SKIP LOCKED`.

A workload that outgrows the first two has outgrown this design, and the honest answer at that
point is a managed broker.
