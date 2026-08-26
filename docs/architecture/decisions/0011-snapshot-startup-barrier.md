# ADR 0011: Barrier snapshot for gap-free hub startup

- Status: Accepted
- Date: 2026-08-26

## Context

A consolidator readiness response can become stale before a hub's state
consumer is assigned. Starting a new consumer at the provider's latest position
and then reading a merely cached snapshot could therefore miss an event between
those two observations. Embedding provider checkpoints in snapshots is outside
the accepted schema.

## Decision

Hub startup first checks consolidator readiness, then establishes its stable
state consumer and buffers through an observed catch-up boundary. Its subsequent
snapshot request is a barrier operation: before responding, the consolidator
consumes and snapshots state through its own end boundary observed during that
request.

An event before the hub boundary is included by the barrier snapshot. An event
after the boundary is available to the hub consumer. Events visible through
both paths are semantic duplicates and do not advance reducer revision twice.
The hub acknowledges its buffered records only after snapshot installation and
successful reduction.

Startup buffering is bounded. If the consumer cannot reach its observed end
within the configured batch bound, startup fails without acknowledging records
instead of installing partial state.

## Consequences

- The protected internal snapshot GET performs synchronization and is not a
  passive cache read.
- The snapshot payload remains provider-neutral and unchanged.
- Consolidator consumption is serialized between tailing and barrier requests.
- Unavailable, malformed, or over-limit startup state keeps the hub unready.
