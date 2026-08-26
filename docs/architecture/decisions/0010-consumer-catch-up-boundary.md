# ADR 0010: Provider-neutral consumer catch-up boundary

- Status: Accepted
- Date: 2026-08-26

## Context

The consolidator must select the persisted snapshot with the greatest revision,
including when the snapshot destination is temporarily empty or a record is
arriving during startup. An empty receive is not a stable replay boundary and
provider offsets cannot enter StateStore or snapshot contracts.

## Decision

MessageStore consumers expose a `CaughtUp` observation, and receive batches may
carry the same signal opportunistically. `CaughtUp` means the consumer has
delivered through the provider's observed end boundary for every assigned
destination shard. It is
a transient observation, not a persisted checkpoint or product identity.

Replay readers continue receiving and acknowledging until `CaughtUp`, then
select by product revision. Tail consumers may use the same signal for
readiness and lag decisions. Providers must prove empty and non-empty catch-up
behavior in the shared conformance suite.

## Consequences

- Kafka compares its assigned consumer position with broker end offsets without
  exposing partitions or offsets outside the provider.
- A concurrently appended record after the observed boundary is handled by
  normal tailing; snapshot revisions remain monotonic.
- Snapshot payloads still contain no provider checkpoint information.
