# ADR 0003: MessageStore, StateStore, and capabilities

- Status: Accepted
- Date: 2026-08-25

## Decision

MessageStore is an internal provider SPI with separate Producer, Consumer, and
Administrator roles. Messages carry product IDs, exact bytes, complete content
types, and safe string metadata. Consumer receipts are opaque internal values;
they are never persisted as product identity or exposed over HTTP.

Consumers report whether they delivered through the provider end boundary
observed by a catch-up check. This transient catch-up signal is
provider-neutral and contains no provider position or identity.

StateStore owns typed lifecycle append and snapshot behavior above
MessageStore. It never imports a provider package. Provider packages implement
the SPI and are wired only by an application composition root.

Every provider classifies durable publish, ordering, durable subscriptions,
acknowledgement, replay, retention, DLQ, delayed delivery, transactions,
provisioning, and consumer scaling as native, product-emulated, restricted, or
unsupported. Configuration validation rejects unmet requirements rather than
silently degrading them.

## Consequences

The conformance suite tests observable send/receive, acknowledgement, negative
acknowledgement, DLQ, reconnect, closure intent, isolation, and capability
behavior. Vendor details remain protected diagnostics, never product identity.
