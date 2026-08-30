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

## Implementation handoff: 30 August 2026

The Kafka BYOB v0.5 preview now includes separate `websubhub` and
`websubhub-consolidator` binaries, TOML configuration with hierarchical
`WEBSUBHUB__` overrides, explicit none/JWT public and operations
authentication, optional hub-to-consolidator mTLS, Kafka-backed MessageStore and StateStore behavior,
resource-topic ingestion and at-least-once delivery, and a two-hub Docker
Compose acceptance topology using `apache/kafka:4.1.0`.

Pull requests #6, #8, #10, #12, #13, #14, and #15 are merged. Release
`v0.5.0` was published on 30 August 2026 from tagged source commit
`bb8ff3d65b1a8f09032c75f12dbc2946ec3e07b2`. Pull requests #16 and #17 are
also merged;
`main` declares `version=0.5.1-SNAPSHOT`.

ADR 0018 fixes lockstep versions with independent component packages. The
release configuration builds 12 archives: `websubhub` and
`websubhub-consolidator` separately for Linux, macOS, and Windows on `amd64`
and `arm64`. Production images are separate Linux `amd64`/`arm64` manifests at
`ayeshalmeida/websubhub` and `ayeshalmeida/websubhub-consolidator`; do not
substitute another Docker Hub namespace. Preview images publish only exact
semantic-version tags, not `latest`. The Docker Hub repositories, scoped
`DOCKERHUB_TOKEN`, protected GitHub `release` environment, repository Actions
pull-request permission, and immutable `v*` tag rules are configured. Native
macOS/Windows signing remains deferred to issue #7.

ADR 0021 supersedes ADR 0016's mandatory inbound JWT rule. Both hub
listeners require an explicit `none` or `jwt` mode; omission is a startup error.
The packaged developer configuration selects `none`, while the production
example and Compose acceptance select `jwt`. JWT configuration is required only
when at least one listener selects it, and invalid JWT configuration never
falls back to `none`. Disabled authentication uses the fixed actor ID
`unauthenticated`; callback verification, SSRF defenses, secret encryption, and
provider security remain active. Existing v0.5.0 configurations must add both
mode declarations when upgrading.

ADR 0020 defines the reviewed version lifecycle through `release.properties`,
the `Prepare release` workflow, and the protected `Release` workflow. Do not
manually create, move, or delete release tags. The first `v0.5.0` attempt
created its immutable tag and then failed because generated release notes made
the checkout dirty. PR #15 moved those notes to `$RUNNER_TEMP` and added a
narrow recovery path for a tag with no published release or artifacts. That
recovery must build the tagged source, may accept only documented
release-automation changes after the tag, and must fail closed for product-code
changes, tag-target mismatches, or an existing release/artifact. `v0.5.0` then
published successfully; this recovery path is no longer applicable to that
version.

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
ordering, a maximum collection limit of 100, exact status filters, and the existing `websubhub:ops:read` scope when `operations.auth.mode = "jwt"`. Cursor pagination is reserved but rejected
in v0.5. `GET /v1/dlq`, tenant/project semantics, management mutations, and the
nested topic-subscriptions convenience route are deferred.

Issue #5 was closed after the management query API shipped. Product-wide
request admission control for both hub and management APIs, plus the
load/isolation benchmark and an agreed publication/delivery regression
threshold, is deferred until after v0.5.0 and tracked by issue #11. Do not add a
management-only throttle, invent a threshold, or claim this performance
acceptance criterion has passed.

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
