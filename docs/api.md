# REST API

The broker's HTTP surface. A machine-readable OpenAPI document is served at `/openapi/v1.json`.

Message bodies are **base64-encoded**, because the broker never parses a payload and so cannot
assume it is text — let alone valid JSON.

## Authentication

Bearer tokens from Entra ID. Three capabilities, each satisfied by a delegated scope (`scp`) or an
application role (`roles`):

| Capability | Grants |
| --- | --- |
| `PubSub.Publish` | Publishing and cancelling scheduled messages |
| `PubSub.Subscribe` | Receiving, settling, and session operations |
| `PubSub.Admin` | Creating and deleting topics, subscriptions, and rules |

Splitting them means a producer that can publish cannot drain a subscription.

## Errors

Errors are [Problem Details](https://www.rfc-editor.org/rfc/rfc9457). The status codes carry
meaning worth relying on:

| Status | Meaning |
| --- | --- |
| 400 | Malformed request, or an invalid filter expression |
| 401 / 403 | Missing or insufficient credentials |
| 404 | No such topic, subscription, or delivery |
| 409 | **The lock was lost** — the message returned and may already be redelivered |
| 503 | The broker cannot reach its database |

409 on settlement is a concurrency outcome, not a fault: someone else owns the message now, and the
work may be repeated. It is deliberately distinct from 404 (the delivery never existed) and 500
(something is broken).

An unexpected 500 does not echo its message back, since the text may describe internals the caller
has no business seeing.

## Publishing

### `POST /topics/{topic}/messages`

Publishes one or more messages as a **single atomic batch** — either all are stored, or none is.

```json
{
  "messages": [
    {
      "messageId": "order-123",
      "subject": "OrderPlaced",
      "correlationId": "trace-abc",
      "sessionId": "customer-7",
      "contentType": "application/json",
      "body": "eyJvcmRlcklkIjoiMTIzIn0=",
      "applicationProperties": { "region": "emea", "total": 750 },
      "scheduledEnqueueTime": "2026-01-01T12:00:00Z",
      "timeToLive": "1.00:00:00"
    }
  ]
}
```

Set `messageId` to a business identifier whenever a retried send should be recognised as the same
message. Left unset, it defaults to a new GUID and every retry becomes a new message.

`applicationProperties` is what subscription filters match on. Keep the values scalar — this is
routing metadata, not a second copy of the payload.

```json
{
  "results": [
    { "sequenceNumber": 42, "wasDuplicate": false, "matchedSubscriptions": 2 }
  ]
}
```

`matchedSubscriptions: 0` is legitimate — no rule matched — but a persistent zero usually means a
filter is wrong.

`wasDuplicate: true` means duplicate detection suppressed the publish; `sequenceNumber` then refers
to the original.

### `DELETE /topics/{topic}/scheduled/{sequenceNumber}`

Cancels a scheduled message that has not yet become visible. Returns `{"cancelled": false}` when it
had already been enqueued — losing that race is an outcome, not an error.

## Receiving

### `POST /topics/{topic}/subscriptions/{subscription}/messages/receive`

Claims messages under a peek-lock, waiting up to `maxWaitTime`.

```json
{ "maxMessages": 10, "maxWaitTime": "00:00:30", "sessionId": null, "receiverId": "worker-1" }
```

An empty result is a normal outcome of long-polling, returned as 200 with an empty list rather than
as an error.

```json
{
  "messages": [
    {
      "deliveryId": 987,
      "lockToken": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
      "lockedUntil": "2026-01-01T12:01:00Z",
      "message": { "sequenceNumber": 42, "deliveryCount": 1, "...": "..." }
    }
  ]
}
```

`deliveryCount` above 1 means an earlier attempt did not settle — treat the work as possibly
already done.

`sessionId` is required on a session-enabled subscription, and the caller must already hold the
session lock.

### `POST .../messages/receive-deferred`

Retrieves deferred messages by sequence number. This is the **only** way back to a deferred
message.

### `POST .../dead-letter/receive`

Reads the dead-letter queue. Does not consume the retry budget, so browsing is free.

## Settling

All take `{"lockToken": "..."}` and return 204, or 409 if the lock was lost.

| Endpoint | Effect |
| --- | --- |
| `POST .../messages/{deliveryId}/complete` | Settled; never delivered again |
| `POST .../messages/{deliveryId}/abandon` | Returned for redelivery; count increments |
| `POST .../messages/{deliveryId}/dead-letter` | Straight to the dead-letter queue |
| `POST .../messages/{deliveryId}/defer` | Set aside; reachable only by sequence number |
| `POST .../messages/{deliveryId}/renew-lock` | Extends the lock; returns the new expiry |

`abandon` accepts `delay` to withhold the message, and `propertiesToModify` to merge properties
before redelivery — useful for recording why the last attempt failed.

`dead-letter` accepts `reason` and `description`. Use it for a message that can never succeed;
retrying one only spends attempts and delays the alert.

### `POST .../dead-letter/replay`

Returns dead-lettered messages with a **fresh delivery budget**. Fix the cause first: replaying
into an unfixed consumer simply refills the queue.

## Sessions

| Endpoint | Effect |
| --- | --- |
| `POST .../sessions/accept` | Takes exclusive ownership. 204 means none available |
| `POST .../sessions/{id}/renew` | Extends the session lock |
| `POST .../sessions/{id}/release` | Releases it for another consumer |
| `GET .../sessions/{id}/state` | Reads stored session state |
| `PUT .../sessions/{id}/state` | Stores session state |

Omit `sessionId` on accept to take whichever session has the oldest unprocessed message. 204 is an
ordinary outcome — every session is busy or empty — not a failure to back off from.

Session state survives a handover, so a consumer resuming a session can read what its predecessor
checkpointed.

## Administration

| Endpoint | Effect |
| --- | --- |
| `GET /topics` | Lists topics |
| `PUT /topics/{topic}` | Creates a topic, or returns the existing one |
| `DELETE /topics/{topic}` | Deletes a topic and everything under it |
| `GET /topics/{topic}/subscriptions` | Lists subscriptions |
| `PUT /topics/{topic}/subscriptions/{name}` | Creates a subscription |
| `DELETE /topics/{topic}/subscriptions/{name}` | Deletes a subscription and its queue |
| `GET .../rules` | Lists rules |
| `PUT .../rules/{name}` | Adds a rule |
| `DELETE .../rules/{name}` | Removes a rule |

`PUT` is idempotent, which makes provisioning safe to run on every deploy. An existing entity's
settings are **not** silently rewritten, so a deliberate operational change survives the next
release.

Creating a subscription without a rule gives it a catch-all, because a subscription with no rules
receives nothing and creating one silently empty would look like a broker fault.

A malformed filter is rejected at creation with 400, so the author of the rule hears about it
rather than a subscriber later wondering where their messages went.

```json
{
  "lockDuration": "00:01:00",
  "maxDeliveryCount": 5,
  "requiresSession": false,
  "rule": { "name": "high-value", "sqlExpression": "region = 'emea' AND total > 500" }
}
```

See [`filter-language.md`](filter-language.md) for the expression grammar.

## Health

| Endpoint | Checks |
| --- | --- |
| `GET /health/live` | The process is running |
| `GET /health/ready` | The process can reach its database |

Both allow anonymous access so an orchestrator can probe them without credentials.
