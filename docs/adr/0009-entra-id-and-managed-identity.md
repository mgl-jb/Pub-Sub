# 9. Entra ID and managed identity, no connection secrets

Status: Accepted

## Context

The broker holds every message in flight between services, which makes it a high-value target and
its credentials worth protecting. It needs to authenticate callers, and it needs to reach a
database and a cache.

Connection secrets are the usual failure: they get committed, shared, copied into ticket comments,
and rotated late or never.

## Decision

Callers authenticate with Entra ID via JWT bearer tokens. Authorization splits into three
capabilities — `PubSub.Publish`, `PubSub.Subscribe`, `PubSub.Admin` — each satisfied by either a
delegated scope or an application role.

The broker reaches SQL and Redis through a user-assigned managed identity. SQL enforces Entra-only
authentication and Redis has access-key authentication disabled outright.

## Consequences

There is no connection secret anywhere: not in the Bicep templates, not in their outputs, not in
container environment variables. Nothing to rotate because nothing is stored.

Splitting capabilities means a credential is scoped to what a service actually does. A producer
that can publish cannot drain a subscription, and neither can delete a topic. Accepting either a
scope or a role lets one policy serve a user-facing app and a daemon using client credentials.

The identity is user-assigned rather than system-assigned so its database and cache grants can be
made before any container app exists; with a system-assigned identity those grants can only follow
app creation, splitting one deployment into two.

Authentication can be disabled for local development, but only by setting
`Broker:DisableAuthentication` explicitly. It **fails closed**: a missing identity-provider
configuration produces a broker that rejects requests, never one that accepts everything. An
unauthenticated broker should not be something you get by forgetting to configure one.

Two costs are worth stating. Bicep cannot execute SQL, so creating the database user for the
managed identity remains a scripted step after deployment — `docs/operations.md` covers it. And
local development against real Azure resources needs a developer signed in to the Azure CLI, which
`DefaultAzureCredential` picks up.

## Alternatives considered

**Connection strings in Key Vault.** Better than environment variables, and still a secret that
exists, is fetched, and must be rotated. Managed identity removes the secret rather than relocating
it.

**API keys for broker callers.** Simple, and no identity provider needed. Rejected for the same
reason: a shared key is a secret to distribute and rotate, and it carries no identity, so an audit
log cannot say who published what.

**System-assigned managed identity.** One fewer resource. Rejected for the two-phase deployment
problem above.
