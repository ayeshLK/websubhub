# ADR 0013: State runtime configuration and ownership

- Status: Accepted
- Date: 2026-08-26

## Context

The initial StateStore used fixed destination names and received retention and
batch bounds as constructor literals. That was sufficient to prove the state
protocol, but it hid deployment policy and made non-default installations
impossible to describe through the canonical process configuration roots.

The hub and consolidator need the same state-event destination, but they do not
own the same behavior. The hub consumes that destination and bounds its startup
buffer. The consolidator provisions state destinations, owns their retention,
and bounds its consumption batches.

## Decision

The hub root contains `state.events.destination`,
`state.events.consumer_batch_size`, and `state.startup.buffer_max`. The
consolidator root contains independently validated `state.events` and
`state.snapshots` destination/retention sections plus
`state.consumer.batch_size`.

Defaults preserve the v0.5 layout: `websub-events`,
`websub-events-snapshots`, seven-day event retention, thirty-day snapshot
retention, batches of 100, and a 1,000-record hub startup buffer. Environment
overrides follow ADR 0008, including paths such as
`WEBSUBHUB__STATE__EVENTS__DESTINATION`.

The consolidator is the provisioning authority. State destinations remain
single-shard product invariants for the preview, and their shard count is not
configurable. Consumer IDs and start positions also remain derived product
invariants. A deployment using a non-default event destination must set the same value in
both process configurations; effective status/capability reporting will make
that value diagnosable when runtime administration is assembled.

StateStore receives provider-neutral destination and retention options. It
does not import the product configuration package, and it continues to expose
no provider offsets, partitions, or consumer-group identities.

## Consequences

- Unknown keys, empty destination names, non-positive bounds, and identical
  event/snapshot destination names fail strict configuration validation.
- Retention and batching policy can be changed without recompiling either
  process.
- Cross-file agreement cannot be validated by either process in isolation;
  deployment tooling and status diagnostics must detect drift.
- Startup synchronization retries, delivery policy, public TLS, and telemetry
  remain separate compatibility decisions and are not implied by this ADR.
