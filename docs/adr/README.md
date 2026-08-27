# Architecture decision records

Each record states the problem, the decision, what it costs, and what was rejected. They are kept
because the alternatives matter: a decision recorded without them reads as arbitrary, and the next
person to revisit it has to rediscover the trade-off from scratch.

Records are immutable once accepted. A decision that changes gets a new record superseding the old
one, rather than an edit that erases the reasoning.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-build-our-own-broker.md) | Build the broker rather than use an Azure messaging service | Accepted |
| [0002](0002-sql-as-system-of-record-redis-as-hot-path.md) | SQL is the system of record; Redis is a hot path | Accepted |
| [0003](0003-rest-long-poll-transport.md) | REST with long-polling, behind a client library | Accepted |
| [0004](0004-sql-like-filter-language.md) | Correlation filters plus a SQL-92-like expression language | Accepted |
| [0005](0005-shared-message-row-per-subscription-delivery.md) | One message row, one delivery row per subscription | Accepted |
| [0006](0006-peek-lock-via-readpast-claim.md) | Peek-lock via a single UPDATE with READPAST | Accepted |
| [0007](0007-redis-is-optional-by-design.md) | Redis is optional by design | Accepted |
| [0008](0008-dotnet-10-lts.md) | Target .NET 10 LTS | Accepted |
| [0009](0009-entra-id-and-managed-identity.md) | Entra ID and managed identity, no connection secrets | Accepted |
| [0010](0010-at-least-once-delivery-and-idempotency.md) | At-least-once delivery, with idempotency as the contract | Accepted |
