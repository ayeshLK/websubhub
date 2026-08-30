# ADR 0017: Internal management query API

- Status: Superseded in part by ADR 0021
- Date: 2026-08-27

## Context

The future control-plane BFF needs safe list and detail views for WebSubHub
topics and subscriptions. The previous subscription inspection endpoint read a
hub-local projection, so its result and revision depended on which hub received
the request. The previous DLQ endpoint exposed a diagnostic prefix without the
pagination, authorization, replay, deletion, and audit semantics required for
a management workflow.

The durable StateStore event log remains the source of truth. The consolidator's
canonical materialized snapshot is the authoritative query view. A successful
durable mutation still does not imply immediate visibility in a hub's local
admission projection, as defined by ADR 0006.

## Decision

ADR 0021 makes the operation-scope requirement below conditional on
`operations.auth.mode = "jwt"`. In `none` mode the same redacted, bounded query
contract is intentionally unauthenticated; all other decisions remain
authoritative.

The protected operations listener exposes this internal, read-only BFF
contract:

```text
GET /v1/topics
GET /v1/topics/{topic_id}
GET /v1/subscriptions
GET /v1/subscriptions/{subscription_id}
```

The collection routes accept a limit from 1 through 100 and exact status
filters. Subscription collections also accept `topic_id`. Cursor pagination is
reserved but rejected in v0.5. Unknown query parameters are rejected.

Every successful response includes the canonical snapshot revision. Retained
inactive topics and removed subscriptions remain inspectable; an unknown stable
product ID returns a bounded 404 response. All routes require `websubhub:ops:read` when the operations listener uses JWT.

The hub authenticates the caller and maps canonical state into explicit API
views. Responses omit content destinations, consumer identities, plaintext and
encrypted subscription secrets, key identifiers, callback user information,
callback capability queries and fragments, authorization values, provider
positions, and provider credentials.

The hub obtains state through a provider-neutral management query boundary
backed by the consolidator client. Consolidator unavailability returns 503;
the handler never falls back to the hub-local projection. The initial v0.5
implementation may obtain the complete bounded snapshot behind that boundary.
Consolidator-side filtering, opaque pagination, caching, and independent query
resource limits can replace it without changing handlers when operating
evidence requires them.

`GET /v1/dlq` is removed from v0.5. Dead-letter persistence remains part of
delivery behavior, but browsing, replay, deletion, authorization, and auditing
require a later contract. `GET /v1/system/capabilities` remains effective
provider and product discovery and reports product replay as unsupported.

This API is an internal control-plane BFF contract, not a supported public
customer administration API. Tenant and project ownership are deliberately
deferred until the namespace, isolation, authorization, identifier, and
migration models are researched and accepted.

## Consequences

- Any healthy hub returns the same canonical topic and subscription revision
  once it can reach the consolidator.
- Management visibility does not prove that a particular hub's local admission
  projection has caught up; clients must still handle retryable conflicts.
- The control-plane BFF does not read consolidator snapshots, StateStore
  records, or Kafka directly.
- List responses remain bounded and deterministically ordered by stable product
  ID.
- A later optimized query service or read model can implement the same internal
  query boundary without exposing persisted schema or provider identity.
