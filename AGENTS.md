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

## Implementation handoff: 31 August 2026

The Kafka BYOB preview now includes separate `websubhub` and
`websubhub-consolidator` binaries, TOML configuration with hierarchical
`WEBSUBHUB__` overrides, explicit none/JWT public and operations
authentication, optional hub-to-consolidator mTLS, Kafka-backed MessageStore
and StateStore behavior, resource-topic ingestion and at-least-once delivery,
and a two-hub Docker Compose acceptance topology using `apache/kafka:4.1.0`.

Releases `v0.5.0` and `v0.6.0` were published on 30 August 2026. The current
release is `v0.6.0`, built from annotated tag `v0.6.0` at source commit
`4e41d3dde79f1f8e909103a519582006dbf7ca62` after PR #21 merged. It introduces
the explicit API authentication modes from PR #18 and remains a pre-1.0
developer preview, regardless of the GitHub release's prerelease flag. `main`
currently declares `version=0.6.1-SNAPSHOT` after PR #22 merged at commit
`3ed798ef80887a1287eb7fc1dfed37d991f3b649`. PR #23 then refreshed the GitHub
product profile, and PR #24 added the Docker Hub product profiles; `main` was
at commit `cb9fca2bb384147de14baa420a0c5f5d7e16b151` after those merges.

PR #23 establishes the current GitHub positioning around "Durable HTTP event
delivery, backed by Kafka today." The qualifier describes the current profile,
not a permanent product-identity constraint; reconsider provider-specific
branding as additional supported profiles ship. The README leads with the
value proposition, current `v0.6.0` developer-preview status, primary links,
comparison context, and an earlier quickstart. The live repository description
and discovery topics are populated. Every release preparation must review the
repository description, topics, README positioning and version status, links,
badges, social preview, and provider-specific wording.

PR #24 adds version-controlled Docker Hub overview sources at
`docs/dockerhub/websubhub.md` and
`docs/dockerhub/websubhub-consolidator.md`. The consolidator description is
"Canonical state and snapshot service for WebSubHub deployments." The
`Sync Docker Hub profiles` workflow owns both short descriptions, publishes
the two overview sources through the protected `release` environment, runs
after a successful `Release` workflow, and supports manual retries. The live
Docker Hub repositories currently have both descriptions and overviews. Keep
the checked-in sources and workflow inputs authoritative; do not make a
live-only profile edit. Docker Hub profile content, tag policy, supported
platforms, component roles, configuration guidance, and security claims must
be reviewed with every release.

ADR 0018 fixes lockstep versions with independent component packages. The
release configuration builds 12 archives: `websubhub` and
`websubhub-consolidator` separately for Linux, macOS, and Windows on `amd64`
and `arm64`. Production images are separate Linux `amd64`/`arm64` manifests at
`ayeshalmeida/websubhub` and `ayeshalmeida/websubhub-consolidator`; do not
substitute another Docker Hub namespace. Preview images publish only exact
semantic-version tags, not `latest`; adding `latest` after the v1.0 stability
boundary is tracked by issue #19. The Docker Hub repositories, scoped
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
version. The same reviewed flow prepared and published `v0.6.0`, then opened
and merged PR #22 for the next snapshot. Do not edit a released version back
to a snapshot on the tagged commit.

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

Kafka client security supports TLS 1.2 or later with an explicit CA and server
verification, optional client-certificate mTLS, and SASL `plain`,
`scram-sha-256`, or `scram-sha-512`. TLS and SASL can be composed, and the hub
and consolidator have independent Kafka identities. The real-broker CI and
Compose acceptance topology currently exercise plaintext Kafka only; secured
broker interoperability, failure cases, and minimum per-component Kafka ACLs
must not be described as integration-qualified. That work is deferred to the
v1.0 stability boundary and tracked by issue #20.

ADR 0023 redefines preview state schema version 1, based on no production
adoption, and makes the normalized topic content type an immutable
resource-topic contract. Registration defaults an omitted content type to
`application/json`; publications must match the complete topic media type and
parameters. Ordinary content messages persist only their stable message ID and
exact body bytes. Delivery obtains `Content-Type` from topic state, and the
canonical management topic DTO exposes it. Upgrading from an earlier preview
requires offline clean-state replacement using new state event and snapshot
destinations followed by topic and subscription recreation. Legacy topic
records without content type fail closed. Earlier-preview and `0.7.0`
processes must not share state destinations even though both use schema version
1.
The feature begins the `0.7.0-SNAPSHOT` minor development line; do not move it
back to the automatically opened `0.6.2-SNAPSHOT` patch line.

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

Preserve exact payload bytes and complete topic content types. Never log payloads,
authorization values, subscription secrets, callback capability queries, or
provider credentials. Tests must avoid timing sleeps.
