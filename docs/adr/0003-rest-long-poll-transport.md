# 3. REST with long-polling, behind a client library

Status: Accepted

## Context

Publishers and subscribers need a way to reach the broker. The transport decides how consumers
learn that work is waiting, how the broker is operated and debugged, and what a client in another
language would have to implement.

## Decision

The broker is its own service exposing a REST API. Receives long-poll: a request waits up to a
configured duration for a message and returns empty if none arrives.

A client library wraps the API so application code sees `IEventPublisher` and
`IMessageHandler<T>`, never an HTTP call.

## Consequences

The API is inspectable with `curl` and any HTTP client, which matters more for a broker than for
most services because "why did this message not arrive?" is asked at three in the morning. A client
in another language needs only an HTTP library.

Long-polling gives most of the latency benefit of push without connection management: an idle
receiver holds one request rather than a persistent connection with its own reconnection and
backpressure semantics.

The costs are per-message HTTP overhead relative to a binary protocol, and a request timeout that
must exceed the long-poll wait — a subtlety worth noting, since getting it wrong makes every idle
receive look like a broker failure and triggers pointless retries.

## Alternatives considered

**REST plus SignalR push.** Lower latency still. Adds connection lifecycle management and a
backplane requirement once the broker scales beyond one instance, for a gain that long-polling
already largely delivers.

**gRPC streaming.** Efficient and strongly typed. A heavier client story, and far more awkward to
inspect during an incident.

**An embedded library with no broker service.** Fewest moving parts and no network hop, since every
app would talk straight to the shared database. Rejected because it leaves nowhere to put central
concerns: admin, dead-letter management, rule compilation, and the sweeper would each have to exist
in every application, and every application would need database credentials.
