# ADR 0001: Product and protocol-library boundary

- Status: Accepted
- Date: 2026-08-25

## Context

`github.com/ayeshLK/lib-websubhub` is a maintained, standard-library-only Go
framework for WebSub protocol mechanics. A deployable broker needs durable
application state, delivery, clustering, policy, security, observability, and
operations that the library intentionally excludes.

## Decision

WebSubHub composes a pinned released version of `lib-websubhub`; it does not
fork or copy the protocol implementation. The library owns bounded protocol
parsing, verification mechanics, callback types, lifecycle handling, and one
delivery attempt. This product owns persistence, fan-out, ordering, retry,
acknowledgement, DLQ behavior, replay, node ownership, security policy,
telemetry, deployment, and upgrades.

Changes to WebSub wire behavior begin in the library. Product code must not
patch around the library contract. Library conformance evidence is necessary
but is not sufficient for a product-level conformance claim.

## Consequences

- `lib-websubhub` remains an external module dependency and will be pinned when
  protocol composition begins.
- Broker or product types must not enter the library merely for convenience.
- Product tests cover the durable behavior before and after library callbacks.
- The product documents limitations such as deferred renewal and lease expiry
  without describing them as library failures.
