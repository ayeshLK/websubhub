# WebSubHub

WebSubHub is an open-source HTTP Event Broker. Publishers use HTTP, WebSubHub
owns durable event and subscription behavior, and verified subscribers receive
at-least-once HTTP push delivery.

The repository is at the initial `v0.5.0` Kafka BYOB preview implementation
stage. It currently contains the repository and quality baseline only; the two
commands expose build identity but do not yet start product runtimes.

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

Go 1.25 or newer is required. The supported baseline is Go 1.25.x, and CI also
tests Go 1.26.x. GNU Make is optional and provides the repository shortcuts
shown below.

```sh
make check
make build
./bin/websubhub --version
./bin/websubhub-consolidator --version
```

Builds may set `VERSION`, `COMMIT`, and `BUILD_DATE`; release automation will
provide them from the protected release workflow.

## Architecture decisions

Accepted decisions and unresolved implementation gates are recorded under
[`docs/architecture/decisions`](docs/architecture/decisions/README.md).

## Security

Do not report vulnerabilities through a public issue. Follow
[`SECURITY.md`](SECURITY.md) instead.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
