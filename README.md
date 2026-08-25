# WebSubHub

WebSubHub is an open-source HTTP Event Broker. Publishers use HTTP, WebSubHub
owns durable event and subscription behavior, and verified subscribers receive
at-least-once HTTP push delivery.

The repository is implementing the `v0.5.0` Kafka BYOB preview. Through Slice
2, it defines provider-neutral persistence contracts, capability validation,
concrete versioned state records, deterministic reduction, snapshots, and
conformance fixtures, plus a franz-go Kafka provider with a real-broker test
profile. The two commands expose build identity but do not yet start runtimes.

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

## Architecture decisions

Accepted decisions and unresolved implementation gates are recorded under
[`docs/architecture/decisions`](docs/architecture/decisions/README.md).

## Security

Do not report vulnerabilities through a public issue. Follow
[`SECURITY.md`](SECURITY.md) instead.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
