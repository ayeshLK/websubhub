# WebSubHub

WebSubHub is an open-source HTTP Event Broker. Publishers use HTTP, WebSubHub
owns durable event and subscription behavior, and verified subscribers receive
at-least-once HTTP push delivery.

The repository is implementing the `v0.5.0` Kafka BYOB preview. Through Slice
5, it defines provider-neutral persistence contracts, capability validation,
concrete versioned state records, deterministic reduction, snapshots, and
conformance fixtures, plus a franz-go Kafka provider with a real-broker test
profile. Slice 3 adds strict TOML configuration, hierarchical environment
overrides, optional internal mTLS, MessageStore-backed StateStore behavior,
barrier snapshots, consolidator readiness, and gap-free buffered hub projection
startup. Slice 4 pins and composes `lib-websubhub` v0.6.0, maps resource-topic
lifecycle callbacks to durable state events, seals subscription secrets behind
an injected boundary, assigns stable product identities, and rejects preview
renewal without scheduling lease expiry. Slice 5 persists exact publisher
representations, runs one sequential consumer per locally owned subscription,
delivers through the pinned library with stable message IDs and HMAC support,
and implements explicit HTTP-managed or MessageStore-managed retry, durable
acknowledgement, stale/removal transitions, reconnect, and DLQ behavior.
Slice 6 requires JWT/JWKS authentication and operation scopes on public and
administrative endpoints, enforces callback destination policy at admission
and dial time, encrypts persisted subscription secrets with a file-backed
AES-256-GCM key, and provides separate health and protected operations
surfaces with bounded redacted inspection and low-cardinality metrics.
Slice 7 composes the two process-specific configurations into runnable hub and
consolidator binaries, adds a two-hub preview topology, and validates durable
state recovery and delivery ownership against the pinned `apache/kafka:4.1.0`
image.

## Product boundaries

WebSubHub keeps two topic contracts distinct:

- WebSub resource topics distribute the current representation of a
  URL-addressed resource through the W3C WebSub lifecycle.
- Event-stream topics will distribute immutable CloudEvents through a separate
  product-owned publication, progress, retention, and replay contract.

The first implementation targets WebSub resource topics using Kafka supplied
by the operator. Renewal, lease expiry, automatic ownership transfer, the
event-stream API, SQLite, IBM MQ, Solace, Native Cluster, the Control Plane, and
the full administration CLI are outside the `v0.5.0` boundary.

## Build

Go 1.25.8 or newer is required, and the latest security patch for the selected
Go release line must be used. CI tests Go 1.25.x, 1.26.x, and 1.27.x. GNU Make
is optional and provides the repository shortcuts
shown below.

```sh
make check
make build
./bin/websubhub --version
./bin/websubhub-consolidator --version
```

Builds may set `VERSION`, `COMMIT`, and `BUILD_DATE`; release automation will
provide them from the protected release workflow.

The portable Kafka suite runs against a real broker when its address is set:

```sh
WEBSUBHUB_TEST_KAFKA_BROKERS=127.0.0.1:9092 \
  go test -v -run '^TestKafkaConformance$' \
  ./internal/persistence/messagestore/kafka
```

The full preview smoke test builds local static binaries, generates ephemeral
two-day test certificates, starts Kafka 4.1.0, the consolidator, two hubs, and a
controlled JWT/subscriber fixture, then removes the test containers and volume:

```sh
make compose-smoke
```

It covers JWT enforcement, resource registration, verified subscriptions,
single-owner signed delivery, MessageStore redelivery, DLQ/stale/removal
outcomes, cross-hub projection consistency, and continued delivery after the
owning hub restarts. Generated credentials remain ignored under
`deploy/compose/.generated/` and are for local acceptance only.

## Configuration

Configuration uses two strict process-specific TOML roots:

- [`configs/websubhub.example.toml`](configs/websubhub.example.toml) contains
  the hub listener, public URL, bounded protocol behavior, stable server ID,
  state projection, consolidator client, delivery policy, and MessageStore.
- [`configs/websubhub-consolidator.example.toml`](configs/websubhub-consolidator.example.toml)
  contains the consolidator listener and MessageStore.

Shared Kafka and TLS value types are implemented once, but each loader rejects
settings owned by the other process. Environment variables override leaf fields
relative to that process's root using double underscores. For example,
`WEBSUBHUB__SERVER__ID=hub-b` overrides the hub's `server.id` and is rejected
by the consolidator.

Internal hub-to-consolidator authentication is explicitly `mtls` or `none`.
The hub's client identity is under `consolidator.auth.mtls`; the
consolidator's server identity and client CA are under `server.auth.mtls`.
mTLS is recommended and verifies both peers. `none` is intended only for an
isolated trusted network and never results from a partial TLS configuration.

Public protocol and operations endpoints do not have an unauthenticated mode in
the v0.5 preview. Configure the exact JWT issuer, audience, HTTPS JWKS URL, and
asymmetric algorithm allowlist under `security.jwt`. Protocol operations use
the scopes recorded in ADR 0016. Liveness and bounded readiness are the only
unauthenticated operations endpoints.

Callback delivery permits public HTTPS destinations on configured ports by
default. Private networks and HTTP require an explicit exact-host/CIDR and port
allowlist. Subscription HMAC secrets are encrypted before state persistence
using the key reference under `security.secrets`; the key file contains 32 raw
bytes or their standard base64 encoding.

## Architecture decisions

Accepted decisions and unresolved implementation gates are recorded under
[`docs/architecture/decisions`](docs/architecture/decisions/README.md).

## Security

Do not report vulnerabilities through a public issue. Follow
[`SECURITY.md`](SECURITY.md) instead.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
