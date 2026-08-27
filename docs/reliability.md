# Reliability

What this system guarantees, what it does not, and where each guarantee is enforced.

## The delivery guarantee

**At-least-once.** A message is delivered until it is settled, and may be delivered more than once.

Exactly-once delivery is not achievable across a process boundary: a consumer that processes a
message and then crashes before acknowledging is indistinguishable, from the broker's side, from
one that crashed before processing. See [ADR 10](adr/0010-at-least-once-delivery-and-idempotency.md).

**Consumers must be idempotent.** This is not an edge case to handle eventually — redelivery
follows routinely from:

- a lock expiring because processing outran it under load;
- a consumer restarting during a deploy with messages in flight;
- a settlement that succeeded but whose acknowledgement was lost;
- an operator replaying the dead-letter queue.

## Making a consumer idempotent

**First choice: natural idempotency.** An operation that produces the same result however many
times it runs needs no bookkeeping:

- an upsert keyed on a business identifier;
- a write that sets an absolute value rather than applying a delta;
- a state transition that is a no-op when already in the target state.

Event payloads can be shaped to enable this. Carrying the resulting state rather than a delta means
a consumer applies an absolute value, which is naturally idempotent. `CreateShipmentHandler` in the
sample worker is written this way deliberately.

**Second choice: the inbox.** When the work cannot be made naturally idempotent, wrap the handler:

```csharp
services.AddIdempotentHandler<OrderPlaced, ChargeCardHandler, BillingDbContext>();
```

The marker is written **in the same transaction** as the handler's own changes, so a crash cannot
leave the work done but unrecorded. Deduplication is enforced by a unique constraint rather than a
prior read, because read-then-write leaves a window in which two concurrent deliveries both find
nothing and both proceed.

The consumer name is part of the key, because the same message legitimately reaches several
subscriptions and one having processed it says nothing about the others.

## What duplicate detection does and does not do

Duplicate detection suppresses a repeated `MessageId` within the topic's window. It covers the
**send** side: a producer whose request timed out, retried, and would otherwise have created a
second message.

It does **not** prevent a consumer from seeing the same message twice. It operates on publish, not
delivery, and within a bounded window. Treating it as sufficient is the most common way to get this
wrong.

## The transactional outbox

A database write and a broker publish cannot be made atomic. Both orderings fail:

- publish first, and a failed save announces something that never happened;
- save first, and a crash before publishing means the change happened and nobody heard.

The window is small enough to look fine in testing and to bite in production.

Staging the publish intent in the same database, in the same transaction, removes the choice —
either both land or neither does — and reduces an impossible atomicity problem to an ordinary
at-least-once one:

```csharp
db.Orders.Add(order);
db.AddToOutbox("orders", new OrderPlaced(...), o => o.MessageId = order.Id);
await db.SaveChangesAsync();   // both, or neither
```

The publisher may send the same message twice if its acknowledgement is lost, which is why the
staged `MessageId` is carried through rather than regenerated: duplicate detection then recognises
the repeat.

A message that fails `MaxAttempts` times is marked `Failed` and left for an operator. Retrying
forever would let one unpublishable message starve everything behind it.

## Retry and dead-lettering

| Situation | What to do | What happens |
| --- | --- | --- |
| Transient failure (downstream timeout) | Throw, or `AbandonAsync` | Redelivered; delivery count increments |
| Needs a delay before retrying | `AbandonAsync(delay: ...)` | Withheld until the delay passes |
| Will never succeed (bad data) | `DeadLetterAsync` | Straight to the dead-letter queue |
| Not this message's turn | `DeferAsync` | Removed from the flow; **record the sequence number first** |

A deferred message is retrievable **only** by sequence number. Deferring without recording it
strands the message until its time to live expires.

`MaxDeliveryCount` is the backstop against a poison message consuming a consumer indefinitely. Note
that a lock lost to expiry counts as an attempt, so a subscription whose lock duration is too short
can dead-letter perfectly good messages.

## Choosing a lock duration

The lock duration should exceed normal processing time, with headroom:

- **too short** — healthy work is redelivered as duplicates, and the delivery budget is spent on
  attempts that were actually succeeding;
- **too long** — a crashed consumer's messages sit idle for the full duration before anyone else
  can take them.

For work that is legitimately slow, prefer automatic lock renewal over a long lock duration. The
processor renews at half the remaining lock, up to `MaxAutoLockRenewalDuration`; that ceiling is
what stops a hung handler from holding a message forever.

`MaxConcurrentCalls` is the other half of this. Every in-flight message holds a lock, so
concurrency far above what the handler can keep up with produces redeliveries rather than
throughput.

## Ordering

Not guaranteed by default: a redelivered message returns behind newer ones.

Where order matters, use sessions. Messages sharing a `SessionId` are delivered to one consumer at
a time, in sequence order, and different sessions proceed concurrently.

The cost is throughput. A session is processed serially, so a slow message blocks the rest of its
session. Choose a key granular enough to keep sessions independent — per customer or per entity,
not per tenant.

## Message expiry

A message whose time to live elapses is dead-lettered by default, and discarded when the
subscription sets `DeadLetterOnMessageExpiration = false`.

Dead-lettering is the default because a message quietly vanishing at expiry is indistinguishable
from one that was lost.

For a scheduled message, the lifetime starts when it becomes **visible**, not when it was
published. Measuring from publish would expire a message scheduled beyond its own time to live
before it could ever be delivered.

## What is not guaranteed

- **Exactly-once delivery.** Not achievable; see above.
- **Global ordering.** Only per-session ordering exists.
- **Cross-region replication.** Single-region. A workload needing this has outgrown the design.
- **Unbounded throughput.** Bounded by one SQL database's write rate.
- **Message ordering after a dead-letter replay.** A replayed message re-enters at the back.

## Where each guarantee is tested

| Guarantee | Test |
| --- | --- |
| One receiver per message | `A_locked_message_is_invisible_to_other_receivers` |
| Competing consumers get disjoint sets | `Competing_consumers_receive_disjoint_message_sets` |
| Stale tokens are rejected | `Settlement_with_a_stale_lock_token_is_rejected` |
| Expired locks redeliver and count | `An_expired_lock_returns_the_message_and_counts_the_attempt` |
| Renewal holds a message | `Renewing_a_lock_keeps_the_message_held` |
| Budget exhaustion dead-letters | `Exceeding_the_delivery_budget_dead_letters_the_message` |
| Replay resets the budget | `Replaying_returns_dead_lettered_messages_with_a_fresh_budget` |
| Browsing the DLQ is free | `Reading_the_dead_letter_queue_does_not_consume_the_retry_budget` |
| Duplicates are suppressed in-window | `A_repeated_message_id_is_suppressed_within_the_window` |
| Detection windows expire | `The_same_message_id_is_accepted_again_after_the_window_lapses` |
| Sessions are exclusive | `Racing_consumers_produce_exactly_one_session_owner` |
| Sessions are ordered | `Messages_in_a_session_are_delivered_in_sequence_order` |
| Session state survives handover | `Session_state_survives_a_handover` |
| Scheduled messages stay hidden | `A_scheduled_message_is_invisible_until_its_time` |
| Scheduled TTL starts at visibility | `A_scheduled_messages_lifetime_starts_when_it_becomes_visible` |
| Expiry dead-letters | `An_expired_message_is_dead_lettered_by_default` |
| The outbox is transactional | `A_rolled_back_transaction_publishes_nothing` |
| The outbox survives an outage | `A_recovered_broker_drains_the_backlog` |
| The outbox gives up eventually | `A_failed_publish_is_retried_with_backoff_and_eventually_gives_up` |
| Redis loss changes nothing | `RedisFallbackTests` (six tests) |

These run against a real SQL Server rather than an in-memory provider, because the behaviour they
assert — `READPAST` claiming, row locking, unique-constraint races — only exists in a real engine.
