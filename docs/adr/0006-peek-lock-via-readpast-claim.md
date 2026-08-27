# 6. Peek-lock via a single UPDATE with READPAST

Status: Accepted

## Context

This is the primitive the whole broker rests on. Several consumers compete for one subscription's
messages, and the system must guarantee that exactly one receiver holds a given message at a time,
that a crashed receiver's message returns rather than being lost, and that consumers scale
horizontally instead of serialising behind each other.

## Decision

Claim messages with a **single statement**: a CTE selecting the next available rows, an `UPDATE`
that locks them, and an `OUTPUT` clause returning what was claimed.

```sql
WITH candidate AS (
    SELECT TOP (@count) *
    FROM   Deliveries WITH (ROWLOCK, READPAST, UPDLOCK)
    WHERE  SubscriptionId = @subId AND State = 0 AND AvailableAt <= @now
    ORDER BY SequenceNumber
)
UPDATE candidate
SET State = 1, LockToken = NEWID(), LockedUntil = @lockedUntil, DeliveryCount = DeliveryCount + 1
OUTPUT inserted.Id, inserted.LockToken, ... INTO @claimed;
```

Settlement then presents `(Id, LockToken)`. A stale token means the lock lapsed and the message has
already gone back.

## Consequences

Each hint earns its place:

- **`UPDLOCK`** takes the update lock at read time rather than upgrading later. Without it two
  receivers can both read the same row before either writes, and both believe they own it.
- **`READPAST`** makes a receiver skip rows another already holds instead of blocking on them.
  Without it competing consumers serialise, and the throughput of N receivers is that of one.
- **`ROWLOCK`** discourages escalation to page or table level, which would block unrelated
  subscriptions sharing the table.

Doing selection and locking in one statement closes the window a `SELECT` followed by an `UPDATE`
would leave open. This is the reason the integration tests run against a real SQL Server: none of
this behaviour exists in an in-memory provider, so a test suite using one would pass while proving
nothing. The load-bearing test runs six receivers concurrently against sixty messages and asserts
their claims are disjoint — it fails without `UPDLOCK` and stalls without `READPAST`.

The lock is time-bounded, so a crashed consumer's messages return once the sweeper notices. The
delivery count is deliberately **not** reset by that, because an attempt that did not settle still
counts: otherwise a consumer that reliably crashes mid-message would retry forever instead of
eventually dead-lettering.

The cost is that this is SQL Server-specific. Porting to PostgreSQL would mean
`FOR UPDATE SKIP LOCKED`, which is the same idea under a different name.

## Alternatives considered

**`SELECT` then `UPDATE`.** The obvious approach, and racy. Two receivers can select the same row.

**Pessimistic locking without `READPAST`.** Correct but serialising: receivers block on each
other's rows, and adding consumers stops adding throughput.

**Optimistic concurrency on a version column.** Workable, but under contention most claims fail and
retry, which converts contention into wasted round trips exactly when load is highest.
