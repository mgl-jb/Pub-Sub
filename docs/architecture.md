# Architecture

A publish/subscribe message broker built on Azure's non-messaging primitives. See
[ADR 1](adr/0001-build-our-own-broker.md) for why it is built rather than bought.

## Component topology

```mermaid
graph TB
    subgraph producers["Producer applications"]
        API["Orders API<br/><i>PubSub.Client</i>"]
        OUT[("Application DB<br/>+ outbox")]
        API -->|"one transaction"| OUT
        OUTPUB["Outbox publisher"]
        OUT --> OUTPUB
    end

    subgraph broker["Broker (Container Apps, 2+ replicas)"]
        REST["REST API<br/><i>publish · receive · settle · admin</i>"]
        CORE["Broker core<br/><i>fan-out · peek-lock · sessions</i>"]
        SWEEP["Sweeper<br/><i>leader-elected</i>"]
        REST --> CORE
        SWEEP --> CORE
    end

    subgraph stores["State"]
        SQL[("Azure SQL<br/><b>system of record</b>")]
        REDIS[("Redis<br/><i>wakeups · leader lease</i>")]
    end

    subgraph consumers["Consumer applications"]
        W1["Worker replica 1"]
        W2["Worker replica 2"]
        INBOX[("Application DB<br/>+ inbox")]
        W1 --> INBOX
        W2 --> INBOX
    end

    OUTPUB -->|"HTTP publish"| REST
    W1 -->|"HTTP long-poll"| REST
    W2 -->|"HTTP long-poll"| REST

    CORE <--> SQL
    CORE -.->|"optional"| REDIS

    classDef authoritative stroke-width:3px
    class SQL authoritative
```

SQL is authoritative for everything. Redis is drawn with a dashed edge because the broker runs
correctly without it — see [ADR 7](adr/0007-redis-is-optional-by-design.md).

## Publish and fan-out

A publish stores the payload **once** and creates one delivery row per matching subscription
([ADR 5](adr/0005-shared-message-row-per-subscription-delivery.md)).

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant R as Rule engine
    participant DB as SQL

    P->>B: POST /topics/orders/messages
    B->>DB: BEGIN TRANSACTION

    opt duplicate detection enabled
        B->>DB: look up (topic, messageId)
        Note over B,DB: A live record suppresses the publish.<br/>A lapsed one is reused, not inserted beside.
    end

    B->>DB: INSERT Messages (payload stored once)
    DB-->>B: SequenceNumber

    loop each subscription on the topic
        B->>R: evaluate rules against the message
        alt a rule matches
            R-->>B: matched (+ optional action)
            B->>DB: INSERT Deliveries
        else no rule matches
            R-->>B: no match — not delivered here
        end
    end

    B->>DB: COMMIT
    B-)Redis: signal waiting receivers
    Note over B,Redis: After the commit, never before:<br/>a receiver woken early finds nothing.
    B-->>P: sequence numbers
```

The whole batch is atomic. Because the connection retries transient faults, the transaction runs
inside EF's execution strategy — otherwise a retry could resume mid-transaction and commit half a
batch.

## Peek-lock: the delivery state machine

```mermaid
stateDiagram-v2
    [*] --> Available: published
    [*] --> Scheduled: published with a future time

    Scheduled --> Available: its time arrives

    Available --> Locked: claimed (delivery count + 1)

    Locked --> Completed: complete
    Locked --> Available: abandon, or the lock expires
    Locked --> Deferred: defer
    Locked --> DeadLettered: dead-letter

    Available --> DeadLettered: delivery budget spent
    Available --> DeadLettered: time to live expires
    Deferred --> Locked: received by sequence number

    DeadLettered --> Available: replayed (count reset)

    Completed --> [*]: pruned after retention

    note right of Locked
        Bounded by the lock duration.
        A crashed consumer's message
        returns when the sweeper notices.
    end note

    note right of DeadLettered
        Terminal until an operator acts.
        Browsing it does not consume
        the retry budget.
    end note
```

An expired lock **does not** reset the delivery count. The attempt was made and did not settle, so
it counts — otherwise a consumer that reliably crashes mid-message would retry forever instead of
eventually dead-lettering.

The claim itself is one atomic statement; [ADR 6](adr/0006-peek-lock-via-readpast-claim.md)
explains each lock hint and what breaks without it.

## Sessions: ordering through exclusivity

```mermaid
sequenceDiagram
    participant C1 as Consumer 1
    participant C2 as Consumer 2
    participant B as Broker
    participant DB as SQL

    C1->>B: accept session
    B->>DB: INSERT SessionLocks (unique on subscription+session)
    DB-->>B: inserted
    B-->>C1: session "customer-a", lock token

    C2->>B: accept session "customer-a"
    B->>DB: INSERT SessionLocks
    DB-->>B: unique constraint violation
    B-->>C2: 204 — busy, try another
    Note over C2,DB: The database arbitrates, not application<br/>logic, so this holds across replicas.

    loop while messages remain
        C1->>B: receive (max 1, session-scoped)
        B-->>C1: next message, in sequence order
        C1->>B: complete
        C1->>B: renew session lock
    end

    C1->>B: release session
    Note over B,DB: The row survives so session state<br/>outlives the consumer that set it.
```

Ordering comes from exclusivity: only the lock holder may claim that session's deliveries, so its
messages are handled one at a time. Concurrency is **across** sessions, never within one — a
session key too coarse (per tenant rather than per entity) serialises far more than intended.

## Redis and its fallbacks

```mermaid
flowchart LR
    PUB["Publish commits"] --> AVAIL{"Redis available?"}
    AVAIL -->|yes| SIG["Signal the channel"] --> WAKE["Receiver wakes immediately"]
    AVAIL -->|no| POLL["Receiver's timed poll<br/>finds it within one interval"]

    SWEEP["Sweeper wants to run"] --> AVAIL2{"Redis available?"}
    AVAIL2 -->|yes| LEASE["SET NX EX lease"]
    AVAIL2 -->|no| APPLOCK["sp_getapplock"]

    WAKE --> OK["Message delivered"]
    POLL --> OK
    LEASE --> SWEPT["Sweep runs once"]
    APPLOCK --> SWEPT
```

Both paths reach the same outcome; only latency differs. Redis pub/sub is fire-and-forget, so a
signal published during a reconnect is simply lost — which is survivable precisely because nothing
depends on it.

## Projects

| Project | Responsibility |
| --- | --- |
| `PubSub.Abstractions` | Envelope, publisher and handler contracts, filters, exceptions. No dependencies. |
| `PubSub.Filters` | Tokenizer, parser, and closure-compiled evaluator for the filter language. |
| `PubSub.Broker.Core` | EF Core model, publish fan-out, the peek-lock claim, settlement, sessions, sweeper. |
| `PubSub.Broker.Redis` | Optional wakeups and leader election. Degrades to the core's defaults. |
| `PubSub.Broker.Api` | HTTP surface, authentication, OpenAPI, health, observability. |
| `PubSub.Client` | `IEventPublisher`, the processor pump, lock renewal, trace propagation. |
| `PubSub.Outbox` | Transactional outbox and inbox deduplication over EF Core. |

## Where the guarantees live

| Guarantee | Enforced by |
| --- | --- |
| One receiver per message | `UPDLOCK` in the claim statement |
| Competing consumers scale | `READPAST` in the claim statement |
| A crashed consumer's work returns | Lock expiry plus the sweeper |
| A poison message stops eventually | Delivery count against the subscription's budget |
| Ordering within a key | A unique index on `(SubscriptionId, SessionId)` |
| A batch is all or nothing | One transaction inside EF's execution strategy |
| A producer's retry is not a second message | Duplicate detection on `MessageId` |
| Data change and announcement share a fate | The outbox, committed with the caller's transaction |
| Reprocessing has no extra effect | The inbox's unique constraint, or natural idempotency |

Every row is covered by a test that fails if the mechanism is removed. See
[`reliability.md`](reliability.md).
