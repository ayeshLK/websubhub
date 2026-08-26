# ADR 0015: Exact content ingestion and delivery state machine

- Status: Accepted
- Date: 2026-08-26

## Context

Slice 5 must turn the accepted content-delivery contract into product behavior
without weakening contiguous acknowledgement, exposing provider identity, or
claiming exactly-once delivery. Publisher content and subscriber delivery have
different durable boundaries and must remain independently recoverable.

## Decision

Content publication accepts only the exact-content publisher mode in v0.5.0.
Event-only notification remains disabled until a bounded, SSRF-safe resource
fetcher exists. The ingestor validates the active projected topic, preserves
the exact bytes and complete content type, assigns a random stable product
message ID, and sends to the topic's provider-neutral content destination.
Publisher success is returned only after that durable send succeeds.

Each active subscription owned by the local stable server ID has one
asynchronous worker and one independent consumer. New consumer identities start
at the provider's latest position; reopening an existing identity resumes its
durable progress. Records are processed sequentially and acknowledged only
after successful HTTP delivery or durable dead-lettering.

The configuration discriminator `delivery.retry.strategy` is exactly
`http` or `message_store`. Only the selected policy executes, so mixed
retry behavior is structurally impossible.

HTTP-managed retry uses bounded attempts, exponential backoff, bounded jitter,
maximum interval, and configured retryable HTTP statuses. Transport failures
are retryable. Exhaustion leaves the record unacknowledged, appends a stale
transition, closes temporarily, and stops. `reset_on_exhaust` begins another
bounded cycle until success, removal, shutdown, or a non-retryable result.

MessageStore-managed retry performs one HTTP attempt. Explicit status mappings
select redelivery, dead-letter, or failure; the configured default applies to
unmapped HTTP results and a separate action applies to transport failures.
Redelivery calls `Nack` with the configured delay. Dead-lettering retains
exact content and safe product metadata, then advances source progress through
the provider's declared boundary. Failure appends stale state and closes
temporarily.

HTTP 410 always appends permanent removal and closes permanently. Malformed
stored messages always dead-letter and are never delivered. Their exact body
is retained, missing or invalid envelope fields are replaced with safe DLQ
values, and stored metadata is reduced to the product allowlist. Receive failures
wait the configured reconnect interval and reconnect without advancing
progress. Shutdown closes temporarily. Unsubscription, topic removal, and HTTP
410 close permanently. Guarded stale reactivation starts the same persisted
consumer identity and therefore resumes its unacknowledged position.

Delivery uses the pinned library for one HTTP attempt and adds the stable
product message ID as `X-Hub-MessageId`. Subscription secrets are opened only
for construction of the immutable delivery client. Logs, errors, DLQ metadata,
and content metadata contain neither plaintext secrets nor callback URLs.

## Consequences

- Delivery remains at least once; a crash after HTTP success and before
  acknowledgement can produce a duplicate.
- Kafka DLQ send and source commit currently have a documented duplicate
  window because they are two durable operations.
- Retry waiting and jitter are injectable in deterministic tests; production
  waits remain context-cancellable.
- Automatic DLQ replay, event-only fetch, renewal, expiry, ownership transfer,
  and fencing remain deferred.
