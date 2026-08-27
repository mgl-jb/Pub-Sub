# 10. At-least-once delivery, with idempotency as the contract

Status: Accepted

## Context

Every messaging system must state its delivery guarantee, because consumers are written against it
whether or not it is written down.

Exactly-once delivery is not achievable across a process boundary. A consumer that processes a
message and then crashes before acknowledging it is indistinguishable, from the broker's side, from
one that crashed before processing it. The broker must choose: redeliver, risking duplicate
processing, or not, risking lost work.

## Decision

**At-least-once.** A message is delivered until it is settled, and a message may be delivered more
than once. Consumers are required to be idempotent, and the system provides the tools to be:

- **Duplicate detection** suppresses repeated message ids within a window, covering producer-side
  retries.
- **The transactional outbox** ensures a data change and its announcement share a fate.
- **The inbox** deduplicates on the consumer side when a handler's work is not naturally idempotent.

The guarantee is documented on `IMessageHandler<T>` itself, where someone writing a handler will
see it.

## Consequences

Every handler must tolerate seeing a message twice. Redelivery is not an edge case: it follows
routinely from lock expiry under load, consumer restarts during deploys, and settlements whose
acknowledgement was lost.

The preferred answer is a naturally idempotent operation — an upsert keyed on a business
identifier, a write that sets an absolute value rather than applying a delta. This needs no
bookkeeping at all, and the sample deliberately demonstrates it as the primary case with the inbox
as the fallback.

Where that is not possible, the inbox writes its marker in the same transaction as the handler's
changes, so a crash cannot leave the work done but unrecorded. Deduplication is enforced by a unique
constraint rather than a prior read, because read-then-write leaves a window in which two concurrent
deliveries both find nothing and both proceed.

Duplicate detection is explicitly *not* a substitute for idempotent handlers. It operates on the
send side and within a bounded window; it says nothing about redelivery to a consumer. Treating it
as sufficient is the most common way to get this wrong.

Ordering is not guaranteed by default — a redelivered message returns behind newer ones. Where
order matters, sessions provide it per key, at the cost of serialising that key's messages.

## Alternatives considered

**At-most-once.** Settle on delivery rather than on completion. Simpler for consumers, and loses
messages whenever one crashes mid-processing. Unacceptable for the order-placement kind of workload
this is built for.

**Claiming exactly-once.** Some systems advertise this. What they deliver is at-least-once delivery
plus deduplication — which is what this provides, without the name that encourages consumers to
skip the idempotency they still need.
