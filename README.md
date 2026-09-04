<div align="center">

# WebSubHub

**Durable HTTP event delivery, backed by Kafka today.**

An open-source, self-hosted HTTP event broker for reliable event delivery
across service, network, and organizational boundaries.

[Quick start](#quick-start) · [Install](docs/installing.md) ·
[Releases](https://github.com/ayeshLK/websubhub/releases) ·
[Architecture](docs/architecture/decisions/README.md) ·
[Contributing](CONTRIBUTING.md)

[![CI](https://github.com/ayeshLK/websubhub/actions/workflows/ci.yml/badge.svg)](https://github.com/ayeshLK/websubhub/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/ayeshLK/websubhub)](https://github.com/ayeshLK/websubhub/releases/latest)
[![Go version](https://img.shields.io/github/go-mod/go-version/ayeshLK/websubhub)](go.mod)
[![License](https://img.shields.io/github/license/ayeshLK/websubhub)](LICENSE)
[![Status: developer preview](https://img.shields.io/badge/status-developer%20preview-orange)](#project-status)

</div>

Publishers use ordinary HTTP. WebSubHub owns durable content, subscriptions,
retries, and delivery progress. Verified subscribers receive at-least-once
HTTP push delivery without integrating with Kafka clients, offsets, consumer
groups, or credentials.

Kafka is the durable engine for the current Bring Your Own Broker (BYOB)
profile, not part of the public integration contract. The product boundary is
designed to support additional persistence providers in the future.

> [!IMPORTANT]
> The current release, [`v0.6.0`](https://github.com/ayeshLK/websubhub/releases/tag/v0.6.0),
> is a pre-1.0 Kafka BYOB developer preview. It implements the WebSub
> resource-topic path and is not a production-readiness claim. Delivery is at
> least once, so subscribers must handle duplicates idempotently.

## Why WebSubHub?

Events often cross team, company, network, and technology boundaries.
Traditional brokers require broker-specific clients and network access, while
webhook systems repeatedly rebuild durability, verification, retry, progress,
dead-letter handling, and endpoint safety.

WebSubHub puts an HTTP-native boundary around those broker responsibilities:

- publishers and subscribers integrate with ordinary HTTP;
- callbacks are verified before a subscription becomes active;
- accepted content is persisted before durable acceptance is returned;
- by default, each subscription owns independent delivery progress and retry behavior;
- provider details remain behind a capability-aware MessageStore contract;
- callback SSRF controls, explicit none/JWT authorization modes, secret protection, health, and
  diagnostics are product responsibilities rather than application glue.

Delivery is deliberately **at least once**. Subscribers must handle duplicates
idempotently; WebSubHub does not claim end-to-end exactly-once delivery.

### Where it fits

| Approach | Integration boundary | Your application owns |
|---|---|---|
| Ordinary webhooks | HTTP | Persistence, callback verification, retries, progress, and dead-letter handling |
| Direct Kafka access | Kafka clients and broker identities | Broker-specific integration and external access policy |
| **WebSubHub** | HTTP publishing and verified HTTP callbacks | Idempotent handling of at-least-once delivery |

WebSubHub is intended for platform teams and service owners that need durable
HTTP event delivery while keeping broker infrastructure behind a controlled
product boundary.

## Quick start

### Prerequisites

- Go 1.26.8 or a newer supported Go release
- Docker with Compose v2
- OpenSSL
- GNU Make (optional, but used by the commands below)

Clone the repository:

```sh
git clone https://github.com/ayeshLK/websubhub.git
cd websubhub
```

To start the topology and keep it running while you register a topic, verify a
subscription, publish content, inspect delivery, and query operations, follow
the [interactive Docker Compose quickstart](docs/compose-quickstart.md).

To run the same topology as an automated acceptance test that cleans up after
itself:

```sh
make compose-smoke
```

The smoke test builds the two services and exercises the Kafka-backed,
two-hub topology, authenticated publishing, verified subscriptions,
at-least-once delivery, retry and dead-letter behavior, shared progress, and
owner restart recovery.

## Two explicit topic contracts

WebSubHub keeps resource distribution and immutable event streams distinct.

| Contract | Intended use | Status |
|---|---|---|
| **WebSub resource topic** | Distribute the current representation of a URL-addressed resource using the W3C WebSub lifecycle | Available in the `v0.6.0` preview |
| **CloudEvents event stream** | Distribute immutable events with broker-owned retention, progress, pause/resume, replay, and DLQ semantics | Public contract is gated and not implemented |

WebSub conformance claims apply only to relevant resource-topic behavior. The
optional publisher-to-hub extension is explicitly non-W3C behavior.

## Preview capabilities

The current implementation provides:

- two runnable Go services: `websubhub` and
  `websubhub-consolidator`;
- strict, process-specific TOML configuration with hierarchical
  `WEBSUBHUB__...` environment overrides;
- Kafka-backed provider-neutral MessageStore and MessageStore-backed
  StateStore behavior;
- deterministic state reduction, revisioned snapshots, and buffered hub
  startup without a snapshot/event gap;
- WebSub topic registration/deregistration and verified subscribe/unsubscribe
  lifecycle persistence;
- topic-governed content types (default `application/json`), publication
  matching, and exact payload-byte preservation;
- sequential per-subscription delivery with stable message IDs and WebSub HMAC
  signatures;
- optional Kafka consumer-group load balancing and explicit partition
  assignment for delivery subscriptions;
- HTTP-managed or MessageStore-managed retry, stale state, HTTP `410 Gone`
  removal, and durable dead-letter handling;
- stable hub ownership so two hubs project all state while only the recorded
  owner delivers a subscription;
- explicit `none` or `jwt` authentication modes for the public and
  operations listeners, with JWT/JWKS scope authorization in secured mode;
- callback policy enforcement at admission and DNS/IP dial time, redirect
  refusal, and bounded HTTP behavior;
- AES-256-GCM encryption of persisted subscription secrets;
- optional mTLS for hub-to-consolidator communication;
- liveness, readiness, capabilities, consolidator-authoritative topic and
  subscription inspection, and low-cardinality Prometheus text metrics;
- a repeatable two-hub acceptance topology using
  `apache/kafka:4.1.0`.

## Architecture

```mermaid
flowchart LR
    P[HTTP publisher] -->|register and publish| H[websubhub]
    S[HTTP subscriber] -->|verified subscribe| H
    H -->|state events and exact content| K[(Kafka)]
    K -->|state events| C[websubhub-consolidator]
    C -->|barrier snapshot| H
    K -->|independent subscription progress| D[owned delivery worker]
    D -->|signed HTTP push| S
```

Every hub consumes the complete product state. A subscription's persisted
`server_id` selects its delivery owner in the preview; automatic takeover and
fencing are deferred.

## Kafka delivery options

Kafka BYOB subscribers can opt into provider-specific delivery behavior with
non-standard subscription form fields:

- `kafka.consumer_group` selects a shared Kafka consumer group. Subscriptions
  using the same value compete for records, so each content record is delivered
  to one of their callbacks rather than independently to every callback.
- `kafka.topic_partitions` selects a comma-separated set of decimal partition
  IDs. Only those operator-provisioned partitions are consumed, and committed
  progress resumes after reconnect or restart.

The fields are mutually exclusive. WebSubHub validates them asynchronously
before callback intent verification. Confirmed subscriber errors produce the
existing `hub.mode=denied` callback; Kafka availability or authorization errors
remain operational failures. These values are not credential fields or product
identities and are omitted from management responses, metrics, and logs.
Delivery remains at least once in both modes.

## Build

Build both services:

```sh
make build
./bin/websubhub --version
./bin/websubhub-consolidator --version
```

Build metadata can be supplied explicitly:

```sh
make build VERSION=v0.6.1-dev COMMIT=local BUILD_DATE=2026-08-31T00:00:00Z
```

The binaries accept a process-specific TOML file:

```text
websubhub --config /path/to/websubhub.toml
websubhub-consolidator --config /path/to/websubhub-consolidator.toml
```

Start Kafka and the consolidator before the hub. The files under
[`configs/`](configs/) are secure configuration templates, not immediately
runnable credentials; copy and adapt them for your broker, identity provider,
TLS identities, and secret mounts.

## Release distributions

Both components share one product version but are packaged and deployed
independently. Every release provides separate `websubhub` and
`websubhub-consolidator` archives for Linux, macOS, and Windows on `amd64` and
`arm64`. An extracted archive has this shape:

```text
<component>_<version>_<os>_<arch>/
├── bin/<component>[.exe]
├── config/<component>.toml
├── README.md
├── LICENSE
└── NOTICE
```

The included TOML is a secure template rather than runnable credentials. Start
Kafka, then the consolidator, then one or more hubs. Use the same release
version for both processes.

Container images are published separately for Linux `amd64` and `arm64`:

```sh
docker pull ayeshalmeida/websubhub:0.6.0
docker pull ayeshalmeida/websubhub-consolidator:0.6.0

docker run --rm ayeshalmeida/websubhub:0.6.0 --version
docker run --rm ayeshalmeida/websubhub-consolidator:0.6.0 --version
```

Runtime deployments must mount the appropriate configuration and secret files
and provide Kafka/network connectivity. Images run as non-root and
intentionally contain only their component application binary. Preview images
publish exact semantic-version tags and do not publish `latest`.

Release assets include SHA-256 checksums, per-archive SPDX JSON SBOMs, keyless
Sigstore signatures, and provenance attestations. Initial macOS and Windows
artifacts do not have native platform signing; that roadmap item is tracked in
[issue #7](https://github.com/ayeshLK/websubhub/issues/7). See the
[installation and deployment guide](docs/installing.md) for binary and
container usage. Maintainers should use the [release guide](docs/releasing.md)
for local dry runs and the publication checklist.

## Test

Run the appropriate level of validation:

| Command | Coverage |
|---|---|
| `make test` | Shuffled Go unit and package tests |
| `make check` | Formatting, generated drift, source headers, vet, shuffled tests, race detector, dependency licenses, and documentation links |
| `make test-integration-kafka` | Kafka provider contract and provider-specific behavior against a real broker |
| `make compose-smoke` | Full two-hub Kafka acceptance topology |

Run the complete Kafka provider integration suite against an existing broker:

```sh
WEBSUBHUB_TEST_KAFKA_BROKERS=127.0.0.1:9092 \
  make test-integration-kafka
```

For focused development, standard Go tooling also works:

```sh
go test ./internal/delivery
go test -race ./...
go vet ./...
```

CI tests the configured Go version matrix and additionally runs Kafka provider
integration, vulnerability scanning, and secret scanning. Third-party GitHub
Actions are pinned to immutable commit SHAs.

## Configuration

WebSubHub uses two strict configuration roots:

- [`configs/websubhub.example.toml`](configs/websubhub.example.toml) configures
  structured logging, the public and operations listeners, stable server ID, explicit authentication
  modes, JWT policy, callback
  safety, subscription-secret provider, state projection, consolidator client,
  delivery, and Kafka MessageStore.
- [`configs/websubhub-consolidator.example.toml`](configs/websubhub-consolidator.example.toml)
  configures structured logging, the internal snapshot service, state behavior, authentication,
  and Kafka MessageStore.

Unknown keys, incomplete security settings, invalid retry combinations, and
settings owned by the other process are rejected. Environment variables
override leaf fields using double underscores:

```sh
WEBSUBHUB__SERVER__ID=hub-b
WEBSUBHUB__SERVER__PUBLIC_URL=https://hub-b.example.com/websub
WEBSUBHUB__LOGGING__LEVEL=debug
WEBSUBHUB__MESSAGE_STORE__KAFKA__BROKERS='["kafka-1:9092","kafka-2:9092"]'
```

Both components write newline-delimited JSON logs to standard error. The
`logging.level` setting accepts `debug`, `info`, `warn`, or `error`, defaults to
`info`, and requires a restart when changed.

Kafka TLS and SASL credentials are loaded through file references rather than
inline example secrets. Hub-to-consolidator authentication is either `mtls`
or `none`; mTLS is recommended, while `none` is intended only for an
isolated trusted network.

## HTTP and operations surfaces

The hub path comes from `server.public_url`. Operational endpoints are served
on the separate `operations.listen` listener:

| Endpoint | Authentication | Purpose |
|---|---|---|
| `GET /health/live` | None | Process liveness |
| `GET /health/ready` | None | Bounded state-projection readiness |
| `GET /v1/system/capabilities` | `operations.auth`: none or JWT scope | Effective provider and preview capabilities |
| `GET /v1/topics` | `operations.auth`: none or JWT scope | Bounded canonical topic summaries |
| `GET /v1/topics/{id}` | `operations.auth`: none or JWT scope | Safe canonical topic detail |
| `GET /v1/subscriptions` | `operations.auth`: none or JWT scope | Bounded canonical subscription summaries |
| `GET /v1/subscriptions/{id}` | `operations.auth`: none or JWT scope | Safe canonical subscription detail |
| `GET /metrics` | `operations.auth`: none or JWT scope | Prometheus-compatible bounded metrics |

The topic and subscription routes are an internal preview contract for a
future control-plane BFF, not a supported public customer administration API.
Their revision comes from the consolidator's canonical materialized snapshot;
StateStore events remain the durable source of truth. Management visibility
does not guarantee that a particular hub's local admission projection has
caught up. Consolidator failure returns `503` without a local-projection
fallback. Collection limits are bounded to 100; cursor pagination is reserved
but not implemented in the current preview.

Both listeners require an explicit `none` or `jwt` authentication mode. The
packaged developer configuration selects `none`; the production example and
Compose acceptance topology select `jwt`. JWT mode requires an exact issuer,
audience, HTTPS JWKS URL, asymmetric algorithm allowlist, and operation scopes.
There is no fallback from an invalid JWT configuration to unauthenticated mode.

## Repository layout

```text
cmd/                              service entry points
configs/                          process-specific configuration examples
deploy/compose/                   local Kafka preview topology
docs/architecture/decisions/     accepted ADRs and open gates
docs/installing.md                release installation and deployment guide
packaging/                        per-component release contents
scripts/                          release layout verification
internal/app/                     composition roots and protocol adapter
internal/persistence/             MessageStore, Kafka, and StateStore
internal/delivery/                per-subscription delivery behavior
internal/security/                JWT, callback policy, and secret protection
test/acceptance/                  end-to-end Compose acceptance test
```

## Project status

The current release is `v0.6.0`, a pre-1.0 Kafka BYOB developer preview. It
publishes separate `websubhub` and `websubhub-consolidator` archives for six
platform and architecture combinations, plus separate multi-architecture
Docker Hub images. `main` continues development toward `v0.6.1`.

The preview is suitable for evaluation and integration feedback, not a claim
of production readiness. Broader request-admission and load/isolation
qualification remains deferred under
[issue #11](https://github.com/ayeshLK/websubhub/issues/11).

Later releases add the gated CloudEvents event-stream contract, renewal and
lease expiry, automatic ownership transfer and fencing, richer administration,
production packaging and recovery qualification, and eventually additional
persistence profiles.

See the [architecture decisions](docs/architecture/decisions/README.md) for
accepted behavior and unresolved gates.

## Contributing

WebSubHub welcomes issues, design feedback, documentation improvements, and
tested code changes. Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing a
public HTTP contract, persisted schema, provider capability, or security
boundary change.

Please report security vulnerabilities privately as described in
[SECURITY.md](SECURITY.md), not through a public issue.

## License

WebSubHub is licensed under the [Apache License 2.0](LICENSE). See
[`NOTICE`](NOTICE) for attribution notices.
