# Implementation plan

> Committed as the record of intent: this is the plan the system was built to, kept so that the
> reasoning behind the structure stays available after the fact. Where the implementation departed
> from it, the ADRs in `adr/` say so and why. See `architecture.md` for what was actually built.

## Context

`mgl-jb/Pub-Sub` is an empty repository (no commits). The goal is a production-shaped
publish/subscribe messaging system in C# on .NET, deployed on Azure, covering the
requirements teams actually hit in production: durable fan-out, filtered subscriptions,
peek-lock settlement, retries, dead-lettering, ordering, scheduled delivery, idempotency,
and observability.

**Key constraint from the user:** do **not** build on Azure Service Bus, Event Grid, or
Event Hubs. We implement those capabilities ourselves. Azure is still the deployment
target — we use its *non-messaging* primitives (Azure SQL, Azure Cache for Redis,
Container Apps, Entra ID, Application Insights) as the substrate.

Confirmed decisions, with the alternatives that were weighed and set aside. Every row
below becomes an ADR committed to the repository (see **Documentation deliverables**).

| # | Decision | Choice | Alternatives rejected |
| --- | --- | --- | --- |
| 1 | Messaging backbone | Build our own broker | Azure Service Bus / Event Grid / Event Hubs — excluded by the user; we reimplement their semantics |
| 2 | Durable store | Azure SQL (system of record) **+** Redis (hot path only) | SQL alone (higher dispatch latency); Cosmos DB (peek-lock and FIFO hand-rolled on ETags); Redis alone (weaker durability, poor fit with the EF Core outbox) |
| 3 | Transport | REST + long-poll behind a client library | SignalR push (connection management, backplane on scale-out); gRPC streaming (heavier client, harder to debug); embedded library with no broker (no central admin, DLQ, or filter evaluation) |
| 4 | Filter language | Correlation filters + SQL-92-like expressions | Correlation-only (insufficient routing power); CloudEvents Subscriptions dialect (less familiar to a Service Bus audience) |
| 5 | Fan-out model | One `Messages` row + one `Deliveries` row per matching subscription | Full payload copy per subscription (N× storage and write amplification) |
| 6 | Claim primitive | `UPDATE ... WITH (ROWLOCK, READPAST, UPDLOCK) ... OUTPUT` | `SELECT` then `UPDATE` (racy); pessimistic locking without `READPAST` (consumers block each other) |
| 7 | Redis role | Strictly an optimization; SQL is authoritative | Redis on the correctness path (adds a second source of truth and a new failure mode) |
| 8 | Ships with | Client library, publisher API, consumer workers, EF Core outbox/inbox, Bicep | — (all four selected) |
| 9 | Features | Peek-lock/retry/DLQ + sessions/ordering + observability + scheduled/deferred | — (all four selected) |
| 10 | Runtime | .NET 10 (LTS) | .NET 11 — still preview; ASP.NET Core 11 has no stable release |
| 11 | Auth | Entra ID + managed identity, no connection secrets | SQL admin passwords / Redis access keys (secret sprawl and rotation burden) |
| 12 | Verification | Install .NET SDK, run SQL Server + Redis in Docker, full E2E | Build-only or no-build — neither proves the messaging semantics actually hold |

**Scope note:** this is a large build. It is sequenced in phases below so a working
vertical slice exists at the end of Phase 3, before the more advanced features land.

## Architecture

```
Publisher app ──┐                        ┌── Consumer worker (competing consumers)
                │   PubSub.Client        │
                ├──► REST + long-poll ───┤
                │                        └── Consumer worker (session-ordered)
                ▼
        PubSub.Broker.Api  ──►  Azure SQL   (durable: messages, deliveries, locks)
                           ──►  Redis       (wakeups, rule cache, leader election)
```

Central design rule: **Redis is never required for correctness.** SQL holds all state.
Redis only (a) wakes long-pollers early, (b) caches compiled filter rules, (c) elects the
sweeper leader. If Redis is unavailable the broker falls back to timed polling, a SQL
`sp_getapplock` leader, and per-instance rule caches. This gets tested explicitly.

### Fan-out model

A publish writes **one** `Messages` row (the body, stored once) plus **one `Deliveries`
row per matching subscription**. Per-subscription state (lock, delivery count, DLQ status)
lives on the `Deliveries` row. This avoids N payload copies while keeping each
subscription's progress independent.

### Peek-lock — the core primitive

Competing consumers claim work with a single atomic statement; `READPAST` lets concurrent
receivers skip each other's locked rows instead of blocking:

```sql
WITH claim AS (
    SELECT TOP (@count) *
    FROM   Deliveries WITH (ROWLOCK, READPAST, UPDLOCK)
    WHERE  SubscriptionId = @subId
      AND  State = 0                       -- Available
      AND  AvailableAt <= SYSUTCDATETIME()
      AND  (@sessionId IS NULL OR SessionId = @sessionId)
    ORDER BY SequenceNumber
)
UPDATE claim
SET    State        = 1,                   -- Locked
       LockToken    = NEWID(),
       LockedUntil  = DATEADD(second, @lockSeconds, SYSUTCDATETIME()),
       DeliveryCount = DeliveryCount + 1
OUTPUT inserted.Id, inserted.LockToken, inserted.MessageId, inserted.DeliveryCount, ...;
```

Settlement then targets `(Id, LockToken)` — a stale token means the lock expired and the
operation fails with `MessageLockLostException`, matching real broker semantics.

## Repository layout

```
PubSub.sln
Directory.Build.props            # net10.0, nullable, warnings-as-errors, analyzers
Directory.Packages.props         # central package management
src/
  PubSub.Abstractions/           # envelope, IEventPublisher, IMessageHandler<T>, filters, exceptions
  PubSub.Filters/                # tokenizer, Pratt parser, AST, compiled evaluator
  PubSub.Broker.Core/            # EF Core model, store, sweeper, session manager
  PubSub.Broker.Redis/           # wakeup channel, rule cache, leader election (all optional)
  PubSub.Broker.Api/             # ASP.NET Core minimal API + auth + OpenAPI + health
  PubSub.Client/                 # publisher, MessageProcessor, SessionProcessor, DI, resilience
  PubSub.Outbox/                 # EF Core transactional outbox + inbox dedup
samples/
  Sample.Orders.Api/             # publisher: order write + outbox in one transaction
  Sample.Shipping.Worker/        # consumers: filtered, failing→DLQ, session-ordered
  Sample.Admin.Cli/              # list entities, peek/replay DLQ
tests/
  PubSub.Filters.Tests/
  PubSub.Broker.Tests/           # Testcontainers: MSSQL + Redis
  PubSub.Client.Tests/
  PubSub.E2E.Tests/              # docker compose stack
infra/                           # Bicep modules + params
deploy/docker-compose.yml
docs/                            # architecture docs + ADRs (see below)
.github/workflows/ci.yml
```

## Documentation deliverables

The architecture and the decisions behind it ship **inside the repository**, not just in
chat. `docs/` is a first-class deliverable, written as the code lands rather than bolted
on at the end.

```
README.md                        # what it is, quickstart, capability matrix, repo map
docs/
  plan.md                        # this implementation plan, committed as the record of intent
  architecture.md                # the full design: components, data flow, fan-out, peek-lock,
                                 #   sessions, scheduling, DLQ, Redis fallback — with mermaid diagrams
  data-model.md                  # ER diagram, every table and index, and why each index exists
  filter-language.md             # grammar (EBNF), operators, system vs application properties,
                                 #   three-valued logic, rule actions, worked examples
  api.md                         # REST contract: endpoints, payloads, status codes, error shapes
  client-library.md              # IEventPublisher / IMessageHandler<T>, DI setup, processor options
  reliability.md                 # delivery guarantees, at-least-once semantics, idempotency,
                                 #   outbox/inbox, retry and DLQ policy, ordering guarantees
  observability.md               # traces, metrics, logs, trace-context propagation, dashboards
  operations.md                  # deploy, scale, DLQ triage and replay, runbooks, tuning knobs
  local-development.md           # docker compose, migrations, running the samples
  adr/
    0001-build-our-own-broker.md
    0002-sql-as-system-of-record-redis-as-hot-path.md
    0003-rest-long-poll-transport.md
    0004-sql-like-filter-language.md
    0005-shared-message-row-per-subscription-delivery.md
    0006-peek-lock-via-readpast-claim.md
    0007-redis-is-optional-by-design.md
    0008-dotnet-10-lts.md
    0009-entra-id-and-managed-identity.md
    0010-at-least-once-delivery-and-idempotency.md
```

Each ADR uses the standard form — **Context / Decision / Consequences / Alternatives
considered** — and records the rejected options from the decision table above, so the
reasoning survives even where the choice later proves wrong. `docs/architecture.md` carries
mermaid diagrams for the component topology, the publish fan-out path, the peek-lock
state machine (`Available → Locked → Completed | Deferred | DeadLettered`), and the
session lifecycle. Documentation is treated as part of "done" for each phase, not a
trailing phase of its own.

## Components

### `PubSub.Abstractions`

`MessageEnvelope`: `MessageId`, `CorrelationId`, `Subject`, `ContentType`, `Body`,
`ApplicationProperties`, `SessionId`, `PartitionKey`, `ScheduledEnqueueTime`,
`TimeToLive`, `ReplyTo`, `To`; broker-assigned `SequenceNumber`, `EnqueuedTime`,
`DeliveryCount`, `LockToken`, `LockedUntil`, `State`.

`IEventPublisher` (`PublishAsync<T>`, `PublishBatchAsync`, `ScheduleAsync`,
`CancelScheduledAsync`), `IMessageHandler<T>`, `MessageContext<T>`
(`CompleteAsync`/`AbandonAsync`/`DeadLetterAsync`/`DeferAsync`/`RenewLockAsync`),
filter types (`CorrelationFilter`, `SqlFilter`, `TrueFilter`, `FalseFilter`), and
exceptions (`MessageLockLostException`, `SessionLockLostException`, `MessageNotFoundException`).

### `PubSub.Filters`

Hand-written tokenizer + recursive-descent/Pratt parser producing an AST, then compiled to
`Func<MessageEnvelope, bool>` via `System.Linq.Expressions` and cached per rule.

- Grammar: `AND OR NOT`, `= <> != > >= < <=`, `LIKE` (`%`/`_`), `IN (...)`,
  `IS [NOT] NULL`, `EXISTS(prop)`, parens, string/number/bool/null literals.
- `sys.` prefix addresses system properties (`sys.Subject`, `sys.CorrelationId`,
  `sys.MessageId`, `sys.SessionId`, `sys.To`, `sys.ReplyTo`, `sys.ContentType`);
  bare identifiers address application properties.
- **SQL three-valued logic** — `NULL` propagates the way SQL does, so filter behaviour
  matches operator expectations rather than C# `bool`.
- Rule actions (`SET prop = expr`, `REMOVE prop`) for match-time message transformation.
- Expression length and AST-depth caps reject pathological input. The expression is
  **parsed by us and never concatenated into database SQL** — no injection surface.

### `PubSub.Broker.Core`

EF Core model: `Topics`, `Subscriptions`, `Rules`, `Messages`, `Deliveries`,
`SessionLocks`, `DedupEntries`, plus indexes tuned for the claim query
(`(SubscriptionId, State, AvailableAt, SequenceNumber)`).

- **Publish** — dedup check (unique filtered index on `(TopicId, MessageId)` inside the
  configured window), insert `Messages`, evaluate every subscription's rules, bulk-insert
  matching `Deliveries`, signal Redis. All in one transaction.
- **Sequence numbers** — monotonic per topic via `IDENTITY`, copied onto each `Delivery`
  so the ordered claim needs no join.
- **Scheduled delivery** — `AvailableAt` in the future; cancel deletes still-unclaimed deliveries.
- **Deferral** — `State = Deferred`, retrievable by sequence number.
- **Sessions** — `SessionLocks(SubscriptionId, SessionId, LockToken, LockedUntil, State)`
  claimed atomically, granting one consumer exclusive ordered access to that session's
  deliveries. Includes session state blob (get/set) and idle-timeout release.
- **Sweeper** (`BackgroundService`, leader-elected) — returns expired locks to Available,
  moves `DeliveryCount > MaxDeliveryCount` to DLQ, expires TTL, releases stale session
  locks, prunes completed rows and dedup entries.
- **Dead-lettering** — `State = DeadLettered` with `DeadLetterReason` / `Description`;
  reasons cover max-delivery-exceeded, TTL expiry, filter-evaluation failure, and
  explicit application dead-letter. Replay re-enqueues a fresh delivery with a reset count.

Raw claim/settle statements go through `FromSqlInterpolated`/`ExecuteSqlInterpolated`
(parameterised); everything else uses normal EF Core.

### `PubSub.Broker.Api`

Minimal API, grouped endpoints:

| Area | Endpoints |
| --- | --- |
| Publish | `POST /topics/{topic}/messages`, `:batch`, `:schedule`, `DELETE /topics/{topic}/scheduled/{seq}` |
| Receive | `POST /subscriptions/{topic}/{sub}/messages:receive` (long-poll: `maxMessages`, `maxWaitTime`), `:receive-deferred` |
| Settle | `POST .../messages/{lockToken}:complete\|:abandon\|:deadletter\|:defer\|:renewlock` |
| Sessions | `POST .../sessions:accept`, `.../sessions/{id}:renew\|:release`, session-state get/set |
| DLQ | `GET .../deadletter` (peek), `POST .../deadletter:replay` |
| Admin | CRUD topics / subscriptions / rules |

Entra ID JWT bearer auth with `PubSub.Publish` / `PubSub.Subscribe` / `PubSub.Admin`
scopes; managed identity for SQL and Redis. Problem Details errors, OpenAPI,
`/health/live` + `/health/ready`.

### `PubSub.Client`

`AddPubSubClient(...)` registers a typed `HttpClient` with Polly (retry with exponential
backoff + jitter, circuit breaker, timeout).

- `MessageProcessor` — background pump with `MaxConcurrentCalls`, prefetch, automatic lock
  renewal up to `MaxAutoLockRenewalDuration`, auto-complete on success, abandon on
  exception, and a `ProcessErrorAsync` hook.
- `SessionProcessor` — accepts sessions, processes each strictly in order, renews the
  session lock, releases on idle.
- `AddHandler<TMessage, THandler>()` with a message-type ↔ subject map.
- **Tracing** — W3C `traceparent`/`tracestate` injected into application properties on
  publish and extracted on consume, so a trace spans producer and consumer. `ActivitySource`
  + `Meter` follow OpenTelemetry messaging semantic conventions; exported to Application
  Insights via `Azure.Monitor.OpenTelemetry.AspNetCore`.

### `PubSub.Outbox`

- **Outbox** — `OutboxMessage` entity; `DbContext.AddToOutbox(...)` enlists the publish
  intent in the caller's transaction so the domain write and the intent commit atomically.
  `OutboxPublisher` background service claims batches with the same `READPAST` pattern,
  publishes, marks sent, backs off on failure, and dead-letters after N attempts.
- **Inbox** — `ProcessedMessage(MessageId, SubscriptionName, ProcessedAt)` with a unique
  key. `IdempotentHandlerDecorator<T>` writes the marker and the business change in one
  transaction; a duplicate-key violation means "already processed" → complete and skip.
  Records are retained past the maximum redelivery window, then pruned.

### Samples

- **`Sample.Orders.Api`** — `POST /orders` writes the order and the outbox row in one
  transaction; further endpoints demonstrate scheduled publish and session-keyed publish
  (`SessionId = customerId`).
- **`Sample.Shipping.Worker`** — a filtered subscription (`SqlFilter` on order total), an
  idempotent handler using the inbox, a deliberately failing handler that demonstrates
  retry → DLQ, and a session consumer proving per-customer FIFO.
- **`Sample.Admin.Cli`** — list entities, peek DLQ, replay DLQ.

### `infra/` (Bicep)

`main.bicep` + modules: `identity.bicep` (user-assigned MI + role assignments),
`sql.bicep` (Azure SQL server + DB, **Entra-only auth**, no SQL admin password),
`redis.bicep` (Azure Cache for Redis), `monitor.bicep` (Log Analytics + App Insights),
`containerapps.bicep` (environment + broker/API/worker apps with KEDA scale rules),
`keyvault.bicep`. Parameter files for `dev` and `prod`.

## Implementation phases

Each phase lands its code, its tests, **and its documentation** together.

1. Solution scaffolding, `Directory.Build.props` / `Directory.Packages.props`, abstractions.
   Commit `docs/plan.md` and the ADRs up front — they are the record of intent for the rest.
2. Filter engine + its unit tests (self-contained, high test value) → `docs/filter-language.md`.
3. Broker core (schema, publish fan-out, peek-lock claim, settle) + API + client library
   → **first working vertical slice: publish and consume end to end.**
   → `docs/architecture.md`, `docs/data-model.md`, `docs/api.md`, `docs/client-library.md`.
4. Reliability: retry, DLQ + replay, dedup, TTL, sweeper, lock renewal → `docs/reliability.md`.
5. Sessions and ordering; scheduled and deferred delivery (extends `reliability.md`,
   adds the session-lifecycle diagram to `architecture.md`).
6. Outbox/inbox and the sample apps → `docs/local-development.md`.
7. Observability (OTel, health, metrics), auth, Redis hot path + degradation tests
   → `docs/observability.md`.
8. Bicep, docker compose, CI → `docs/operations.md`, `README.md`; final docs pass to
   reconcile every document against the code as actually built.

## Verification

The container has no .NET SDK. `dot.net` and `builds.dotnet.microsoft.com` are blocked by
the proxy, but **`packages.microsoft.com` is reachable (200)** — install via the Microsoft
apt feed for Ubuntu 24.04. Fallback if that fails: build and test inside
`mcr.microsoft.com/dotnet/sdk:10.0` (MCR confirmed reachable). NuGet and Docker Hub are
both reachable; the machine has 15 GB RAM, 4 CPUs, and 30 GB free disk.

1. Install the .NET 10 SDK; `dotnet build` the solution warnings-clean.
2. `dotnet test` unit suites — filter parser/evaluator (precedence, three-valued logic,
   `LIKE`, `IN`, malformed and hostile input), client processor, idempotent decorator.
3. `docker compose up` SQL Server + Redis; apply EF migrations; run the Testcontainers
   integration suite asserting the behaviours that matter:
   - fan-out reaches exactly the subscriptions whose rules match;
   - **competing consumers receive disjoint message sets** under concurrency;
   - an expired lock redelivers and increments `DeliveryCount`;
   - exceeding `MaxDeliveryCount` dead-letters, and replay re-enqueues;
   - duplicate `MessageId` within the window is suppressed;
   - a scheduled message is invisible until `AvailableAt`, and cancel works;
   - a session grants exclusive access and delivers strictly in order;
   - defer then receive-by-sequence-number round-trips;
   - **with Redis stopped, everything above still passes** (fallback path).
4. E2E over docker compose: `POST /orders` on the Orders API → assert the Shipping worker
   processed it, that the failing handler's message lands in the DLQ, and that a single
   trace spans publisher and consumer.
5. `dotnet format --verify-no-changes`; lint Bicep with `az bicep build` if the CLI can be
   installed (it is absent — otherwise CI covers it).
6. Documentation review: every ADR present and filled in, `docs/api.md` matching the
   generated OpenAPI document, `docs/data-model.md` matching the EF migrations, and the
   quickstart in `README.md` followed literally on a clean checkout to confirm it works.
7. Commit to `claude/pubsub-csharp-dotnet-azure-gax10m` and push. No PR unless asked.

Deliverables are verified by running them; anything that cannot be run in this container
will be called out explicitly rather than reported as passing.
