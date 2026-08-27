# 7. Redis is optional by design

Status: Accepted

## Context

[ADR 2](0002-sql-as-system-of-record-redis-as-hot-path.md) adds Redis to accelerate dispatch. That
creates a risk worth naming: a component added for latency quietly becoming one the system cannot
run without, discovered only during an incident.

Redis pub/sub is fire-and-forget. A signal published while a subscriber is reconnecting is simply
gone. If message delivery depended on those signals, messages would be lost.

## Decision

Redis is an accelerator with no authority. It does two things — wake long-polling receivers, and
elect the sweeper leader — and both have permanent fallbacks: an in-process notifier, and a SQL
application lock.

Every Redis failure degrades rather than propagating. A broker with Redis unconfigured,
unreachable, or mid-failover behaves identically apart from dispatch latency.

## Consequences

A lost wakeup costs one poll interval, because the receiver's timed poll finds the message
regardless. A lost leadership lease costs a duplicated sweep, which is harmless because the sweep is
idempotent. Neither loses a message.

This also makes the leadership lease deliberately *not* a distributed lock. A plain `SET NX EX`
suffices precisely because correctness does not depend on it, and treating it as a lock protecting
correctness would be a mistake — the well-known failure modes of Redis-based locking do not apply
to a lease whose loss is merely wasteful.

Redis optionality is expressed in the type system rather than as a nullable service: a
`RedisConnection` wrapper forces every consumer to decide what to do when it is absent, instead of
discovering it through a null reference.

The claim is tested, not asserted. Six tests exercise the broker against a connection representing
"no Redis" — the same state reached when it is down, misconfigured, or not deployed — and confirm
local wakeups still fire, waits time out rather than hang, leadership falls through to the
database, and a receiver finds its message by polling when no signal ever arrives.

The cost is that the fallback path must be kept exercised, or it rots. Running it in the test suite
is what prevents that.

## Alternatives considered

**Requiring Redis.** Simpler code: one path instead of two. Rejected because it makes a latency
optimisation into an availability dependency, and turns a Redis outage into a broker outage.

**Redis Streams as the delivery mechanism.** Streams are durable and their pending-entries list maps
neatly onto peek-lock. Rejected because it would make Redis a second system of record, splitting
authority between two stores and reintroducing exactly the atomicity problem the outbox exists to
avoid.
