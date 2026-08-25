# ADR 0002: Storage profiles and Kafka-first release

- Status: Accepted
- Date: 2026-08-25

## Context

Kafka, IBM MQ, Solace, SQLite, and a future native replicated engine have
different durability, ordering, replay, retention, acknowledgement, DLQ,
provisioning, and scaling semantics. Presenting them as interchangeable would
silently weaken the HTTP Event Broker contract.

## Decision

WebSubHub keeps one profile-neutral HTTP product contract and follows three
explicit persistence profiles:

1. Bring Your Own Broker, with Kafka as the only `v0.5.0` and first-GA path.
2. Single-process SQLite Standalone, initially experimental.
3. A separately funded Native Cluster program that qualifies only when a
   production multi-node deployment needs no external broker.

An internal MessageStore SPI exposes Producer, Consumer, and Administrator
behavior. StateStore remains a separate product layer persisted through
MessageStore. Providers declare effective capabilities, and startup rejects a
requested policy whose semantics are unavailable. Provider identifiers never
become public product identities.

## Consequences

- Kafka is implemented below `internal/persistence/messagestore/kafka`.
- StateStore never imports Kafka.
- A provider must pass observable conformance scenarios, not merely compile
  against an interface.
- IBM MQ and Solace require design partners and real qualification environments.
- SQLite carries no multi-node, shared-filesystem, or HA claim.
- Native Cluster is not a local queue or a replicated happy-path prototype.
