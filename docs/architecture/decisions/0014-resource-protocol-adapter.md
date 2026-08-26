# ADR 0014: Resource protocol adapter and preview lifecycle

- Status: Accepted
- Date: 2026-08-26

## Context

Slice 4 composes the maintained protocol framework with product-owned durable
state. The adapter must not copy wire mechanics into the product, expose
provider identities, persist plaintext subscription secrets, or make a durable
append appear synchronously visible in every hub projection.

The current released framework is `github.com/ayeshLK/lib-websubhub` v0.6.0.
It includes the publisher-declared registration content type added after the
v0.5.0 execution handoff was written.

## Decision

The product pins `lib-websubhub` v0.6.0 and builds its resource-topic handler
from the library's `Handler` and typed `Service` callbacks.

Stable product topic and subscription IDs are SHA-256 mappings of
length-delimited exact protocol values. Topic IDs use the exact topic URL.
Subscription IDs use the exact topic URL, exact callback URL, and original
verification `LeaseStartedAt`. Delivery consumer IDs use the same input but a
different namespace, so a provider consumer identity is never the product
subscription identity.

Publisher registration and deregistration callbacks append concrete topic
events. Verified subscription and unsubscription callbacks append concrete
subscription events. They never mutate the local projection. Registration
content type is not added to state schema version 1; the exact content type of
each subsequently persisted representation is authoritative.

Subscription secrets pass through a required application-owned sealing
boundary before durable append. State events contain only ciphertext and a key
identifier. Plaintext is never placed in state records or errors.

Admission consults the local projection. A missing prerequisite or an existing
active or stale subscription returns a retryable HTTP 409 response. If
concurrent admissions both append before projection convergence, the reducer
coalesces later non-removed subscriptions for the same exact topic and callback
as semantic duplicates. Successful append means durable acceptance, not
immediate projection visibility, as defined by ADR 0006.

The publisher extension is disabled by default and enabled only by explicit
configuration. Its content callback delegates to a product content sink; until
Slice 5 supplies that sink, publication is denied rather than acknowledged
without persistence.

Active renewal and stale reactivation are rejected in v0.5.0. No lease-expiry
scheduler is started. Unsubscribe followed by a later verified subscription
receives a new product and consumer identity from its new
`LeaseStartedAt`.

## Consequences

- Product tests exercise the HTTP handler and durable callback boundary; the
  library's own conformance suite remains necessary but not sufficient.
- Callback and topic SSRF policy, public authentication, and authorization
  remain blocked on the initial security-profile gate before public exposure.
- A future persisted topic registration content-type policy requires an
  explicit state-schema migration.
- Renewal, expiry, and stale reactivation require a later lifecycle ADR and
  offline migration boundary.
