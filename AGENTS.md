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

## Required workflow

Read the relevant ADRs before changing behavior. Record a new ADR before
introducing a public contract, persisted schema, provider capability, security
boundary, or compatibility rule. Unknown persisted versions require explicit
offline migration; runtime startup must not silently mutate schemas.

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
