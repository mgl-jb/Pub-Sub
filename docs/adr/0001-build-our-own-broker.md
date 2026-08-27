# 1. Build the broker rather than use an Azure messaging service

Status: Accepted

## Context

The system needs publish/subscribe messaging on Azure with the capabilities teams actually hit in
production: durable fan-out, filtered subscriptions, peek-lock settlement, retries, dead-lettering,
ordering, scheduled delivery, and idempotency support.

Azure offers three services that cover much of this — Service Bus, Event Grid, and Event Hubs — and
under ordinary circumstances Service Bus topics would be the obvious answer.

The requirement is explicit that none of them may be used.

## Decision

Implement the broker ourselves, reproducing the semantics of an enterprise message broker, and
deploy it on Azure's non-messaging primitives: SQL Database, Cache for Redis, Container Apps, and
Entra ID.

The semantics are modelled on Service Bus deliberately. It is the reference implementation of this
problem, its behaviour is well documented, and matching it means the concepts transfer: anyone who
knows peek-lock, dead-lettering, and sessions already knows how this behaves, and a future
migration onto Service Bus would be a transport change rather than a redesign.

## Consequences

**Costs.** Everything a managed broker does for free is now ours: the sweeper that returns expired
locks, duplicate detection, the dead-letter lifecycle, session exclusivity, schema migrations, and
the operational burden of a stateful service. Correctness under concurrency has to be proven rather
than assumed, which is why the test suite runs against a real database engine rather than an
in-memory provider — the behaviour that matters, `READPAST` claiming and unique-constraint races,
only exists in a real engine.

**Benefits.** The semantics are inspectable and adjustable. Messages live in tables an operator can
query with familiar tools. There is no per-operation billing, and no service quota to design
around.

**Boundaries.** Two things are explicitly out of scope: geo-replication, and the throughput ceiling
of a single SQL database. A workload that needs either has outgrown this design, and the honest
answer at that point is a managed broker.

## Alternatives considered

**Azure Service Bus topics.** The right answer absent the constraint. Rejected by requirement.

**Event Grid.** Push delivery over webhooks with CloudEvents. Weaker on ordering, dead-letter
ergonomics, and competing-consumer pull semantics, all of which are core here.

**Event Hubs.** Partitioned event streaming with consumer groups and checkpointing. Built for
high-throughput telemetry ingest, not per-message settlement and per-message retry.
