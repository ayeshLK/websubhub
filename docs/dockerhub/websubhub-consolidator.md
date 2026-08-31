# WebSubHub Consolidator

**Canonical state and snapshot service for WebSubHub deployments.**

The consolidator deterministically reduces durable WebSubHub state events and
serves the canonical snapshots used by hub processes. The current Kafka Bring
Your Own Broker (BYOB) profile stores those state events in Kafka while keeping
provider details behind the WebSubHub persistence boundary.

> **Developer preview:** `0.6.0` is a pre-1.0 Kafka BYOB preview, not a
> production-readiness claim. Run the consolidator with the same exact version
> as every `websubhub` instance in the deployment.

## What is this image?

`ayeshalmeida/websubhub-consolidator` contains only the internal consolidator
process. It:

- consumes and deterministically reduces durable state events;
- publishes revisioned canonical snapshots;
- supplies the startup barrier used by hub state projections;
- serves the authoritative topic and subscription query view to hubs.

The consolidator is not a public WebSub endpoint and does not deliver
notifications. A usable Kafka BYOB topology requires Kafka, at least one
consolidator, and one or more same-version
[`ayeshalmeida/websubhub`](https://hub.docker.com/r/ayeshalmeida/websubhub)
instances.

## Quick start

Pull the current preview and inspect its embedded build identity:

```console
docker pull ayeshalmeida/websubhub-consolidator:0.6.0
docker run --rm ayeshalmeida/websubhub-consolidator:0.6.0 --version
```

The image intentionally contains no default configuration or credentials. For
a working Kafka, consolidator, and two-hub evaluation topology, use the
repository's
[interactive Docker Compose quickstart](https://github.com/ayeshLK/websubhub/blob/main/docs/compose-quickstart.md).

## Run the consolidator

Start Kafka first. The following command illustrates the intended container
boundary; the referenced network, configuration, and secrets must already
exist:

```console
docker run -d \
  --name websubhub-consolidator \
  --network websubhub \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --mount type=bind,src="$PWD/websubhub-consolidator.toml",dst=/etc/websubhub/config.toml,readonly \
  --mount type=bind,src="$PWD/secrets",dst=/run/secrets,readonly \
  ayeshalmeida/websubhub-consolidator:0.6.0 \
  --config /etc/websubhub/config.toml
```

The command intentionally publishes no consolidator port. The image metadata
exposes port `8081`, but the effective listener comes from configuration and
should remain on an internal network. Use mTLS for hub clients unless the
traffic stays within an isolated, trusted boundary.

## Configuration

Start from the
[`websubhub-consolidator` example](https://github.com/ayeshLK/websubhub/blob/main/configs/websubhub-consolidator.example.toml)
and configure:

- the `debug`, `info`, `warn`, or `error` structured log level;
- the internal snapshot listener and authentication mode;
- Kafka brokers, state destinations, and snapshot behavior;
- optional Kafka TLS, mTLS, and SASL settings;
- hub-to-consolidator mTLS identities when the internal listener is not
  isolated.

Environment variables override TOML leaves using the `WEBSUBHUB__` prefix and
double underscores between nested keys. Credentials, keys, and certificates
should be mounted as files. Invalid or incomplete security configuration fails
closed.

The container writes newline-delimited JSON logs to standard error. Override
the default `info` level with `WEBSUBHUB__LOGGING__LEVEL`; changes require a
container restart.

## Availability boundary

The consolidator's canonical snapshot is the authoritative management query
view. Hubs do not fall back to a hub-local projection when the consolidator is
unavailable. Successful canonical visibility is separate from a particular
hub's local admission and delivery readiness.

## Tags and platforms

- Images support Linux `amd64` and `arm64`.
- Preview releases publish exact semantic-version tags only.
- There is no `latest`, major, or major/minor preview tag.
- Deploy the same exact version of the consolidator and every hub.
- After verification, production automation should pin an image digest.

## Security and supply chain

The image is minimal, has no shell, and runs as a non-root user. Releases
include BuildKit SBOM and provenance attestations, GitHub provenance, and a
keyless Sigstore signature over the image digest. See the
[consumer verification guide](https://github.com/ayeshLK/websubhub/blob/main/docs/releasing.md#consumer-verification)
before recording deployment digests.

Do not expose the consolidator listener publicly. Use a private network,
read-only mounts, an orchestrator-managed secret store, and explicit resource
limits for deployed instances.

## Resources

- [GitHub repository](https://github.com/ayeshLK/websubhub)
- [Installation and deployment](https://github.com/ayeshLK/websubhub/blob/main/docs/installing.md)
- [Docker Compose quickstart](https://github.com/ayeshLK/websubhub/blob/main/docs/compose-quickstart.md)
- [Releases](https://github.com/ayeshLK/websubhub/releases)
- [Architecture decisions](https://github.com/ayeshLK/websubhub/tree/main/docs/architecture/decisions)
- [Security policy](https://github.com/ayeshLK/websubhub/blob/main/SECURITY.md)
- [Apache License 2.0](https://github.com/ayeshLK/websubhub/blob/main/LICENSE)
