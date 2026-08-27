# Operations

## Deploying

```bash
az deployment group create \
  --resource-group rg-pubsub-prod \
  --template-file infra/main.bicep \
  --parameters infra/main.prod.bicepparam
```

### The step Bicep cannot do

Bicep creates the managed identity and grants it Azure-level access, but it cannot execute SQL. The
database user must be created once, by someone connected as the Entra administrator:

```sql
CREATE USER [<identity-name>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [<identity-name>];
ALTER ROLE db_datawriter ADD MEMBER [<identity-name>];
ALTER ROLE db_ddladmin  ADD MEMBER [<identity-name>];
```

`db_ddladmin` is needed because the broker applies its own migrations at startup. Where that is
unacceptable, run migrations from a deployment pipeline instead and drop the role.

The identity name comes from the deployment's `identityName` output.

## Health

| Endpoint | Meaning |
| --- | --- |
| `/health/live` | The process is running. Does **not** check the database. |
| `/health/ready` | The process can reach its database. |

Liveness deliberately ignores the database: restarting the process would not fix a database
outage, and would turn a brief blip into a restart loop. Readiness does check it, so an instance
that cannot reach SQL stops receiving traffic without being killed.

## Metrics worth alerting on

| Metric | Watch for |
| --- | --- |
| `pubsub.client.handler_failures` | A rising rate means messages are heading for the dead-letter queue |
| `pubsub.client.handler.duration` | Approaching the lock duration means redeliveries are imminent |
| `pubsub.client.settled` (`settlement=abandon`) | Sustained abandons mean a downstream problem |
| Dead-letter queue depth | Any sustained growth needs a human |

The most useful early signal is handler duration approaching the lock duration. It precedes lock
expiry, which precedes duplicate processing, which precedes budget exhaustion and dead-lettering —
alerting on the first gives time to react before any of the rest happens.

## Triaging the dead-letter queue

```bash
export PUBSUB_BROKER_URI=https://<broker-fqdn>
dotnet run --project samples/Sample.Admin.Cli -- dlq <topic> <subscription>
```

Each message shows its `deadLetterReason`:

| Reason | Meaning | Usual action |
| --- | --- | --- |
| `MaxDeliveryCountExceeded` | Retries exhausted | Find the failure in the logs; fix, then replay |
| `TimeToLiveExpired` | Not consumed in time | Check whether consumers were down or too slow |
| `DeserializationError` | The payload did not match the handler's type | A contract mismatch; fix the producer or consumer |
| `FilterEvaluationError` | A subscription rule threw | Fix the rule |
| `ApplicationError` | The handler rejected it deliberately | Read the description |

Browsing does not consume the retry budget, so inspecting is free.

**Fix the cause before replaying.** A replay resets the delivery count, so replaying into an
unfixed consumer simply refills the queue.

```bash
dotnet run --project samples/Sample.Admin.Cli -- replay <topic> <subscription>
```

## Scaling

**Consumers** are the usual thing to scale. Add replicas; the broker hands each a disjoint set of
messages. Note that a consumer's load is its backlog, which no HTTP metric can see — scaling on
request concurrency would leave it at one replica under any backlog. A KEDA scaler reading queue
depth is the right answer; until one is configured, a fixed replica count is honest about that.

**The broker** scales on HTTP concurrency and never to zero: it holds long-polling connections and
runs the sweeper, and a cold start would stall dispatch for everyone.

**SQL** is the ceiling. Watch DTU or vCore utilisation. Serverless suits bursty loads; a workload
that keeps the database warm should move to provisioned, since autopause then buys nothing and
costs a cold start.

## Tuning

| Setting | Raise it when | Lower it when |
| --- | --- | --- |
| `LockDuration` | Handlers legitimately run long | A crashed consumer's messages sit idle too long |
| `MaxDeliveryCount` | Failures are usually transient | Poison messages should surface sooner |
| `MaxConcurrentCalls` | Consumers are idle and the backlog grows | Locks expire during processing |
| `PrefetchCount` | Round-trip latency dominates | Prefetched messages expire before being handled |
| `LongPollInterval` | Only if Redis is absent and load is low | Latency matters and Redis is absent |
| `SweepInterval` | Load is low | Expired locks should return sooner |

Prefetch and concurrency interact: prefetched messages hold locks from the moment they are claimed,
so a large prefetch with slow processing expires locks that a smaller one would not.

## Incident: Redis is down

Nothing to do. Dispatch latency rises to at most one poll interval and the sweeper falls back to a
SQL application lock. No message is lost and no message is duplicated beyond the normal
at-least-once contract.

## Incident: the database is unreachable

The broker returns 503 and readiness fails, so instances stop taking traffic without being killed.
Publishers using the outbox keep accepting work — their intents accumulate locally and drain when
the broker returns, which is the outage the outbox exists for. Publishers calling the broker
directly will fail, which is the argument for the outbox.

## Incident: a consumer is stuck on one message

Look at `DeliveryCount` in the logs. A message climbing toward `MaxDeliveryCount` will dead-letter
itself, which is the system working as intended. To stop it sooner, dead-letter it explicitly from
the handler, or lower the subscription's `MaxDeliveryCount`.

## Backup and recovery

The broker's state is entirely in SQL, so Azure SQL's automated backups cover it. Point-in-time
restore recovers messages in flight as well as entity definitions.

Topics, subscriptions, and rules are configuration and should be provisioned from code — the sample
worker demonstrates declaring them at startup, which keeps a subscription's filter next to the
handler that depends on it.
