# WebSubHub repository guidance

## Product boundary

This repository implements the WebSubHub product. It composes the maintained
`github.com/ayeshLK/lib-websubhub` protocol framework and owns persistence,
delivery, clustering, security, operations, packaging, and release behavior.
Do not move those responsibilities into the library.

Maintain two explicit contracts: WebSub resource topics and CloudEvents
event-stream topics. WebSub conformance claims apply only to relevant resource
topic behavior. Delivery is at least once; never claim end-to-end exactly once.

## Initial release boundary

The first version is a `v0.5.0` Kafka BYOB developer preview with
`websubhub` and `websubhub-consolidator`. Renewal, lease expiry, automatic
ownership transfer, event-stream APIs, SQLite, IBM MQ, Solace, Native Cluster,
Control Plane, and a full CLI are deferred.

Keep MessageStore provider-neutral, with Kafka implementations below
`internal/persistence/messagestore/kafka`. StateStore owns concrete state
behavior over MessageStore and must not import Kafka. Do not expose provider
offsets, partitions, group IDs, handles, endpoint IDs, or credentials as
product identities.

## Implementation handoff: 28 August 2026

The Kafka BYOB v0.5 preview now includes separate `websubhub` and
`websubhub-consolidator` binaries, TOML configuration with hierarchical
`WEBSUBHUB__` overrides, JWT-protected public mutations and operations, optional
hub-to-consolidator mTLS, Kafka-backed MessageStore and StateStore behavior,
resource-topic ingestion and at-least-once delivery, and a two-hub Docker
Compose acceptance topology using `apache/kafka:4.1.0`.

The current handoff is pull request
`https://github.com/ayeshLK/websubhub/pull/6`, branch
`feat/management-query-api`, commit
`41e72970dbcf276a50f43356c6140922f380d248`. All PR checks passed before the
AGENTS handoff update was added.

PR #6 implements the protected internal control-plane BFF query contract:

- `GET /v1/topics`;
- `GET /v1/topics/{id}`;
- `GET /v1/subscriptions`;
- `GET /v1/subscriptions/{id}`.

StateStore events remain the durable source of truth and the consolidator
canonical snapshot is the authoritative query view. Management handlers must
not fall back to a hub-local projection when the consolidator is unavailable.
Successful canonical visibility is not a local admission or delivery barrier;
acceptance tests must synchronize local delivery readiness separately. Topic
IDs can be URLs and must be percent-encoded when placed in the detail path.

The query API uses explicit redacted DTOs, stable product IDs, deterministic
ordering, a maximum collection limit of 100, exact status filters, and the
existing `websubhub:ops:read` scope. Cursor pagination is reserved but rejected
in v0.5. `GET /v1/dlq`, tenant/project semantics, management mutations, and the
nested topic-subscriptions convenience route are deferred.

Issue #5 should remain open after PR #6 until the management polling workload
and acceptable publication/delivery regression threshold are agreed. Dedicated
management-query concurrency/rate limits and a load/isolation benchmark are
still required. Do not invent a threshold or claim this performance acceptance
criterion has passed.

## Required workflow

Read the relevant ADRs before changing behavior. Record a new ADR before
introducing a public contract, persisted schema, provider capability, security
boundary, or compatibility rule. Unknown persisted versions require explicit
offline migration; runtime startup must not silently mutate schemas.

Every Go source and test file must begin with the repository Apache-2.0
copyright header.

Before handing off code, run:

```sh
gofmt -w .
go vet ./...
go test -shuffle=on ./...
go test -race ./...
make license-check docs-check
git diff --check
```

Preserve exact payload bytes and complete content types. Never log payloads,
authorization values, subscription secrets, callback capability queries, or
provider credentials. Tests must avoid timing sleeps.
