# WebSubHub

WebSubHub is an open-source HTTP Event Broker. Publishers use HTTP, WebSubHub
owns durable event and subscription behavior, and verified subscribers receive
at-least-once HTTP push delivery.

The repository is implementing the `v0.5.0` Kafka BYOB preview. Through Slice
3, it defines provider-neutral persistence contracts, capability validation,
concrete versioned state records, deterministic reduction, snapshots, and
conformance fixtures, plus a franz-go Kafka provider with a real-broker test
profile. Slice 3 adds strict TOML configuration, hierarchical environment
overrides, optional internal mTLS, MessageStore-backed StateStore behavior,
barrier snapshots, consolidator readiness, and gap-free buffered hub projection
startup. Runtime composition with the public WebSub handler follows in Slice 4.

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

## Configuration

The canonical schema is TOML; see
[`configs/websubhub.example.toml`](configs/websubhub.example.toml). Environment
variables override leaf fields with double underscores, so
`WEBSUBHUB__SERVER__ID=hub-b` overrides `server.id`. Unknown keys and partial
mTLS settings fail validation.

Internal hub-to-consolidator authentication is explicitly `mtls` or `none`.
mTLS is recommended and verifies both peers. `none` is intended only for an
isolated trusted network and never results from a partial TLS configuration.

## Architecture decisions

Accepted decisions and unresolved implementation gates are recorded under
[`docs/architecture/decisions`](docs/architecture/decisions/README.md).

## Security

Do not report vulnerabilities through a public issue. Follow
[`SECURITY.md`](SECURITY.md) instead.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
