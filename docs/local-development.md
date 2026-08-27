# Local development

## Running the whole system

```bash
cp deploy/.env.example deploy/.env      # change the password
docker compose -f deploy/docker-compose.yml up --build
```

That starts SQL Server, Redis, the broker, the orders API, and two shipping-worker replicas. The
worker creates the topic and its subscriptions on startup, so nothing needs provisioning by hand.

Place an order:

```bash
curl -X POST http://localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","region":"emea","total":750}'
```

Watch it flow through:

```bash
docker compose -f deploy/docker-compose.yml logs -f shipping-worker
```

A total above 500 also matches the `high-value` subscription, so the same order appears twice in
the logs from two different handlers — that is fan-out working, not a duplicate.

| Service | Address |
| --- | --- |
| Broker | http://localhost:8080 |
| Broker OpenAPI | http://localhost:8080/openapi/v1.json |
| Orders API | http://localhost:8081 |
| SQL Server | localhost:1433 |
| Redis | localhost:6379 |

Authentication is disabled in compose via `Broker__DisableAuthentication`. It is opt-in, so a
missing identity-provider configuration fails closed rather than producing an open broker.

## Running from source

Requires the .NET 10 SDK and Docker.

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

> `dotnet test` runs on Microsoft.Testing.Platform, selected in `global.json`. It forwards
> unrecognised arguments to the test executable, so a VSTest-era flag such as `--nologo` fails with
> "Unknown option" and reports zero tests rather than an obvious error.

Where Docker Hub is unreachable, set `TESTCONTAINERS_RYUK_DISABLED=true` to skip the reaper
sidecar; the fixtures dispose their own containers.

## Trying things by hand

```bash
BROKER=http://localhost:8080

curl -X PUT $BROKER/topics/demo -H 'content-type: application/json' -d '{}'

curl -X PUT $BROKER/topics/demo/subscriptions/big-orders \
  -H 'content-type: application/json' \
  -d '{"rule":{"name":"big","sqlExpression":"total > 100"}}'

curl -X POST $BROKER/topics/demo/messages \
  -H 'content-type: application/json' \
  -d "{\"messages\":[{\"subject\":\"Demo\",\"body\":\"$(echo -n '{"hello":"world"}' | base64)\",\"applicationProperties\":{\"total\":500}}]}"

curl -X POST $BROKER/topics/demo/subscriptions/big-orders/messages/receive \
  -H 'content-type: application/json' \
  -d '{"maxMessages":10,"maxWaitTime":"00:00:05"}'
```

Message bodies are base64 because the broker never parses a payload and so cannot assume it is
text.

Settle what you received, using its `deliveryId` and `lockToken`:

```bash
curl -X POST $BROKER/topics/demo/subscriptions/big-orders/messages/<deliveryId>/complete \
  -H 'content-type: application/json' \
  -d '{"lockToken":"<lockToken>"}'
```

## The admin CLI

```bash
export PUBSUB_BROKER_URI=http://localhost:8080
dotnet run --project samples/Sample.Admin.Cli -- topics
dotnet run --project samples/Sample.Admin.Cli -- dlq orders shipping
dotnet run --project samples/Sample.Admin.Cli -- replay orders shipping
```

## Migrations

```bash
dotnet tool install --global dotnet-ef
dotnet ef migrations add <Name> --project src/PubSub.Broker.Core --output-dir Storage/Migrations
```

The broker applies its migrations at startup, so no separate step is needed to run it.

## Watching a failure path

To see retry and dead-lettering, publish an order with a negative total. `ValidatingOrderHandler`
dead-letters it immediately rather than retrying, because no number of attempts makes a negative
total valid:

```bash
curl -X POST http://localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-2","region":"emea","total":-1}'

dotnet run --project samples/Sample.Admin.Cli -- dlq orders validation
```

## Working without Redis

Stop it and confirm nothing breaks:

```bash
docker compose -f deploy/docker-compose.yml stop redis
```

Publishing and consuming continue. Dispatch latency rises to at most one poll interval, and the
sweeper falls back to a SQL application lock. This is [ADR 7](adr/0007-redis-is-optional-by-design.md)
in practice.
