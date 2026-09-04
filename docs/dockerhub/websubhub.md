# WebSubHub

**Durable HTTP event delivery, backed by Kafka today.**

WebSubHub is an open-source, self-hosted HTTP event broker. Publishers use
ordinary HTTP, WebSubHub owns durable content and subscription behavior, and
verified subscribers receive at-least-once HTTP push delivery without
integrating with Kafka clients, offsets, consumer groups, or credentials.

> **Developer preview:** `0.7.0` is a pre-1.0 Kafka Bring Your Own Broker
> (BYOB) preview, not a production-readiness claim. Delivery is at least once;
> subscribers must handle duplicates idempotently.

## What is this image?

`ayeshalmeida/websubhub` contains the public WebSubHub process. It:

- serves the public WebSub and protected operations HTTP listeners;
- registers resource topics and verifies subscriptions;
- persists exact publisher content before durable acceptance;
- projects canonical state and delivers signed HTTP notifications;
- manages per-subscription progress, retries, stale state, and dead letters.

This image is one component of a deployment, not a standalone all-in-one
broker. A usable Kafka BYOB topology requires:

1. Kafka;
2. a same-version
   [`ayeshalmeida/websubhub-consolidator`](https://hub.docker.com/r/ayeshalmeida/websubhub-consolidator);
3. one or more `ayeshalmeida/websubhub` instances.

The hub and consolidator are independently deployable but use one lockstep
WebSubHub version.

## Quick start

Pull the current preview and inspect its embedded build identity:

```console
docker pull ayeshalmeida/websubhub:0.7.0
docker run --rm ayeshalmeida/websubhub:0.7.0 --version
```

The image intentionally contains no default configuration or credentials. For
a working two-hub evaluation topology, use the repository's
[interactive Docker Compose quickstart](https://github.com/ayeshLK/websubhub/blob/main/docs/compose-quickstart.md).

## Run the hub

Start Kafka and the consolidator first. The following command illustrates the
container security and configuration boundary; the referenced network,
configuration, and secrets must already exist:

```console
docker run -d \
  --name websubhub \
  --network websubhub \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  -p 8080:8080 \
  -p 127.0.0.1:9090:9090 \
  --mount type=bind,src="$PWD/websubhub.toml",dst=/etc/websubhub/config.toml,readonly \
  --mount type=bind,src="$PWD/secrets",dst=/run/secrets,readonly \
  ayeshalmeida/websubhub:0.7.0 \
  --config /etc/websubhub/config.toml
```

The image metadata exposes ports `8080` and `9090`; the effective listener
addresses come from the mounted TOML configuration. Keep the operations
listener on a protected management boundary.

## Configuration

WebSubHub uses strict, process-specific TOML configuration. Start from the
[`websubhub` example](https://github.com/ayeshLK/websubhub/blob/main/configs/websubhub.example.toml)
and configure:

- the `debug`, `info`, `warn`, or `error` structured log level;
- the stable server identity, public URL, and listener addresses;
- explicit `none` or `jwt` modes for both public and operations listeners;
- Kafka brokers plus optional TLS, mTLS, and SASL settings;
- subscription-secret encryption, callback policy, retry, and retention;
- the consolidator endpoint and optional hub-to-consolidator mTLS.

Environment variables override TOML leaves using the `WEBSUBHUB__` prefix and
double underscores between nested keys. Credentials, keys, and certificates
should be mounted as files rather than embedded in the image or configuration.
Invalid or incomplete security configuration fails closed.

The container writes newline-delimited JSON logs to standard error. Override
the default `info` level with `WEBSUBHUB__LOGGING__LEVEL`; changes require a
container restart.

## Tags and platforms

- Images support Linux `amd64` and `arm64`.
- Preview releases publish exact semantic-version tags only.
- There is no `latest`, major, or major/minor preview tag.
- Deploy the same exact version of the hub and consolidator.
- After verification, production automation should pin an image digest.

## Security and supply chain

The image is minimal, has no shell, and runs as a non-root user. Releases
include BuildKit SBOM and provenance attestations, GitHub provenance, and a
keyless Sigstore signature over the image digest. See the
[consumer verification guide](https://github.com/ayeshLK/websubhub/blob/main/docs/releasing.md#consumer-verification)
before recording deployment digests.

Do not expose a listener using authentication mode `none` unless another
trusted boundary protects it. Callback verification, SSRF defenses, secret
encryption, and provider security remain active in every authentication mode.

## Resources

- [GitHub repository](https://github.com/ayeshLK/websubhub)
- [Installation and deployment](https://github.com/ayeshLK/websubhub/blob/main/docs/installing.md)
- [Docker Compose quickstart](https://github.com/ayeshLK/websubhub/blob/main/docs/compose-quickstart.md)
- [Releases](https://github.com/ayeshLK/websubhub/releases)
- [Architecture decisions](https://github.com/ayeshLK/websubhub/tree/main/docs/architecture/decisions)
- [Security policy](https://github.com/ayeshLK/websubhub/blob/main/SECURITY.md)
- [Apache License 2.0](https://github.com/ayeshLK/websubhub/blob/main/LICENSE)
