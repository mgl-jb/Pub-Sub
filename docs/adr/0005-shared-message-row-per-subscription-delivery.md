# 5. One message row, one delivery row per subscription

Status: Accepted

## Context

Fan-out means one published message reaches every subscription whose rules match it. Each
subscription then tracks that message independently: its own lock, its own delivery count, its own
dead-letter status. A message one subscriber is still retrying must have no bearing on a subscriber
that already completed it.

The question is what to store per subscription.

## Decision

A publish writes **one** `Messages` row holding the payload, and **one `Deliveries` row per
matching subscription** holding that subscription's state. The delivery references the message.

`SequenceNumber` and `SessionId` are copied onto the delivery rather than joined, so the claim
query can filter and order without touching the messages table.

## Consequences

The payload is stored once regardless of fan-out. A 256 KB message reaching ten subscriptions costs
256 KB, not 2.5 MB, and one insert of it rather than ten.

Each subscription's progress stays fully independent, which is what the model requires.

The denormalisation of sequence number and session id is deliberate: the claim is the hottest query
in the system, and keeping it single-table lets one index serve its predicate and its ordering.

Two costs follow. Pruning a message must wait until no delivery references it, so the sweeper
checks that rather than deleting on age alone. And where a rule action rewrites properties for one
subscription, that subscription's copy is stored on its delivery row — the common case stores
nothing extra, since most rules have no action.

## Alternatives considered

**A full copy of the message per subscription.** Simpler: no join, no denormalisation, and pruning
is per-row. Rejected because storage and write cost scale with the fan-out factor for no benefit —
subscriptions never modify the payload.

**A single message row with per-subscription state in a JSON column.** Avoids the extra table.
Rejected outright: the claim query would have to read and rewrite a document under concurrency,
which is precisely the atomic row-level operation SQL is good at and JSON manipulation is not.
