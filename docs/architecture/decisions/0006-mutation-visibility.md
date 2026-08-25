# ADR 0006: Durable mutation visibility

- Status: Accepted for v0.5.0
- Date: 2026-08-25

## Decision

A successful mutation means its state event crossed the configured durable
append boundary. It does not mean every hub projection exposes the mutation.
Only state consumers mutate local projections.

The preview orders events in the configured state destination. Reducer revision
counts effective transitions; it is not a producer-assigned revision or
provider offset. Concurrent equivalent events converge through semantic
idempotence. An immediate dependent request may receive a retryable conflict
until its prerequisite becomes locally visible.

Authoritative conditional multi-writer admission is deferred hardening and may
not be simulated by trusting a stale local projection.
