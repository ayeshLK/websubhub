# ADR 0004: State records, revisions, and snapshots

- Status: Accepted
- Date: 2026-08-25

## Decision

State schema version 1 uses concrete event records for topic registration,
topic deregistration, verified subscription, verified unsubscription, stale
transition, HTTP 410 removal, and guarded stale reactivation. Every record has
an event ID, UTC occurrence time, and safe actor metadata.

The reducer is deterministic and semantic-idempotent. A transition changes
state only when it changes the addressed entity; exact duplicate events are
no-ops. Effective transitions increment one global revision and stamp the
changed entity. Topics and removed subscriptions remain as tombstones.

Snapshot schema version 1 contains only complete topic state, complete
subscription state, and global revision. Maps encode as ID-sorted arrays for
byte-deterministic JSON. It contains no provider offsets, consumer receipts,
group names, credentials, or replay checkpoints.

Unknown versions fail decoding. Upgrades use explicit offline migration;
startup does not rewrite persisted data.
