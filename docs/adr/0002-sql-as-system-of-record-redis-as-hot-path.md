# 2. SQL is the system of record; Redis is a hot path

Status: Accepted

## Context

Building the broker ourselves ([ADR 1](0001-build-our-own-broker.md)) means choosing what it stores
messages in. The store has to provide durability, atomic claim under concurrency, ordering, and
efficient queries for scheduled and expiring messages.

A purely SQL broker has a latency problem. A receiver has no way to learn that a message arrived
except by asking, so dispatch latency is bounded by the poll interval: poll tightly and burn
queries, poll slowly and add delay to every message.

## Decision

Azure SQL Database is the system of record for everything — entities, messages, delivery state, and
locks. Azure Cache for Redis is added purely to accelerate dispatch: it wakes long-polling
receivers across broker instances and elects the sweeper leader.

Redis holds no state the broker cannot rebuild from SQL.

## Consequences

Durability, atomicity, and ordering come from a database engine built for them, rather than from
application code layered over a store that lacks them. The transactional outbox becomes natural,
because the application's data and its publish intent can share a transaction.

Dispatch latency drops from "up to one poll interval" to "as soon as the publish commits", without
Redis becoming something the system cannot run without. That property is load-bearing enough to
have its own record ([ADR 7](0007-redis-is-optional-by-design.md)) and its own tests.

The cost is two stateful dependencies to deploy, though only one to reason about: a Redis failure
degrades latency, never correctness.

The throughput ceiling is a single SQL database's write rate. Sharding topics across databases is
possible but not implemented.

## Alternatives considered

**SQL alone.** Simplest, and entirely correct. Rejected only for dispatch latency — and the design
keeps this as the permanent fallback, so the simple configuration remains a supported one.

**Cosmos DB.** Scales horizontally and globally. Peek-lock, per-key FIFO, and dead-letter semantics
would all have to be hand-rolled on optimistic concurrency, which is materially more code in the
part of the system that most needs to be right.

**Redis alone.** Very fast, and its streams offer consumer groups with a pending-entries list that
maps well onto peek-lock. Durability is weaker, and it co-exists poorly with an EF Core outbox,
which needs the application's data and the publish intent in one transaction.
