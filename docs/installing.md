# Installation and deployment

WebSubHub's Kafka BYOB preview consists of two independently deployable
components released under one version:

- `websubhub-consolidator` reduces durable state events and serves canonical
  snapshots;
- `websubhub` serves the public and operations endpoints, projects state,
  ingests content, and delivers notifications.

A usable deployment requires Kafka and at least one instance of each
component. Always deploy the same WebSubHub version for both processes. This
guide uses the current `0.6.0` developer preview in its examples.

## Choose a distribution

Binary archives are available for both components on six targets:

| Operating system | Architectures | Format |
|---|---|---|
| Linux | `amd64`, `arm64` | `.tar.gz` |
| macOS | `amd64`, `arm64` | `.tar.gz` |
| Windows | `amd64`, `arm64` | `.zip` |

Container images support Linux `amd64` and `arm64`. Docker selects the matching
platform from the manifest automatically:

```text
docker.io/ayeshalmeida/websubhub:0.6.0
docker.io/ayeshalmeida/websubhub-consolidator:0.6.0
```

The preview does not publish `latest`, major, or major/minor tags. Production
automation should pin the resolved image digest after verifying its signature
and provenance.

## Install binary archives

Download both component archives for the same version and target from the
GitHub Release. With the GitHub CLI, a Linux `amd64` installation can be
downloaded with:

```sh
gh release download v0.6.0 \
  --repo ayeshLK/websubhub \
  --pattern 'websubhub_0.6.0_linux_amd64.tar.gz' \
  --pattern 'websubhub-consolidator_0.6.0_linux_amd64.tar.gz' \
  --pattern 'websubhub_0.6.0_checksums.txt' \
  --pattern 'websubhub_0.6.0_checksums.txt.sigstore.json'
```

Verify the checksums, Sigstore bundle, and GitHub provenance before extracting
the archives. The exact commands and workflow identity are in the
[consumer verification section](releasing.md#consumer-verification).

Extract the archives into separate directories:

```sh
tar -xzf websubhub_0.6.0_linux_amd64.tar.gz
tar -xzf websubhub-consolidator_0.6.0_linux_amd64.tar.gz
```

Each directory contains only its component:

```text
<component>_0.6.0_<os>_<arch>/
├── bin/<component>[.exe]
├── config/<component>.toml
├── README.md
├── LICENSE
└── NOTICE
```

Copy each TOML template into an operator-owned configuration directory. Do not
place credentials in the distribution directory or directly in TOML. The
templates expect sensitive values, private keys, and certificates to be
mounted as files.

Before startup, replace the example values for:

- Kafka bootstrap brokers, TLS identity, and optional SASL authentication;
- the hub's stable server ID, public URL, listener addresses, and explicit
  `server.auth.mode` and `operations.auth.mode`;
- JWT issuer, audience, JWKS URL, algorithms, and operation scopes when either
  listener uses `jwt`;
- subscription encryption key file;
- hub-to-consolidator endpoint and mTLS identities;
- callback allowlists, retry policy, retention, and destination names.

Starting with v0.6.0, both hub listener modes are required. Existing v0.5.0
configurations must add `[server.auth]` and `[operations.auth]` with an explicit
`none` or `jwt` mode. An omitted or unknown mode fails startup. `none` is for
local evaluation or a separately protected trusted boundary; it authorizes all
operations on that listener and emits a startup warning.

Environment variables override TOML using the `WEBSUBHUB__` prefix and
double underscores between nested keys. For example:

```sh
WEBSUBHUB__SERVER__ID=hub-a \
  ./bin/websubhub --config /etc/websubhub/websubhub.toml
```

Both processes emit newline-delimited JSON logs to standard error. Configure
`logging.level` as `debug`, `info`, `warn`, or `error` (default `info`), or use
`WEBSUBHUB__LOGGING__LEVEL=debug`. Log-level changes require a restart.

Start services in dependency order:

1. Kafka;
2. `websubhub-consolidator`;
3. one or more `websubhub` instances.

```sh
./bin/websubhub-consolidator \
  --config /etc/websubhub/websubhub-consolidator.toml

./bin/websubhub --config /etc/websubhub/websubhub.toml
```

Confirm the embedded version before deployment and readiness afterwards:

```sh
./bin/websubhub-consolidator --version
./bin/websubhub --version
curl --fail http://127.0.0.1:9090/health/ready
```

The consolidator also exposes `/health/live` and `/health/ready` on its
configured listener. The packaged hub configuration explicitly selects `none`
for both authentication modes so local evaluation does not require tokens.
Select `jwt` and configure `security.jwt` before exposing either listener. Use
mTLS between hubs and the consolidator unless that traffic stays on an
isolated, trusted network.

## Deploy container images

Pull both exact-version images:

```sh
docker pull docker.io/ayeshalmeida/websubhub:0.6.0
docker pull docker.io/ayeshalmeida/websubhub-consolidator:0.6.0
```

The images are minimal, run as a non-root user, contain no shell, and include
only their component binary plus license notices. They do not contain default
configuration or credentials. Mount the process-specific configuration and
secrets read-only, and ensure the image's non-root user can read every mounted
file.

The following commands illustrate the container boundary. The referenced
network, Kafka service, configuration files, and secrets must already exist.
Set the hub's `server.listen` to `:8080` and `operations.listen` to `:9090`
inside the container. Binding the operations listener to `127.0.0.1` inside
the container prevents Docker port publishing from reaching it; restrict the
host side or use a private management network instead.

The commands intentionally publish no consolidator port:

```sh
docker run -d \
  --name websubhub-consolidator \
  --network websubhub \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --mount type=bind,src="$PWD/websubhub-consolidator.toml",dst=/etc/websubhub/config.toml,readonly \
  --mount type=bind,src="$PWD/secrets",dst=/run/secrets,readonly \
  docker.io/ayeshalmeida/websubhub-consolidator:0.6.0 \
  --config /etc/websubhub/config.toml

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
  docker.io/ayeshalmeida/websubhub:0.6.0 \
  --config /etc/websubhub/config.toml
```

Do not expose the consolidator listener publicly. Bind the operations listener
to a protected management network; the loopback host-port example above is
only reachable from the local host. Set resource limits and use an
orchestrator-managed secret store for non-evaluation deployments.

The repository's `deploy/compose/compose.yaml` is an acceptance topology. It
builds a local test image containing an additional fixture binary and uses
generated credentials plus plaintext Kafka. It is intentionally not a
production deployment definition.

## Verify container identity

Inspect the embedded version:

```sh
docker run --rm docker.io/ayeshalmeida/websubhub:0.6.0 --version
docker run --rm \
  docker.io/ayeshalmeida/websubhub-consolidator:0.6.0 --version
```

Verify both image signatures and provenance before recording immutable
digests in deployment manifests. See
[consumer verification](releasing.md#consumer-verification) for the required
Cosign certificate identity and GitHub attestation commands.

## Upgrade and rollback boundary

The preview uses lockstep component compatibility. Roll out the consolidator
and hubs as one planned version change; do not deliberately run mixed versions
without a compatibility statement in that release's notes. Back up and test
the Kafka deployment according to its operational policy before upgrading.

Release archives are immutable and version tags are never moved. A rollback
therefore selects the previously verified archive or image digest. Whether a
state rollback is safe depends on the release-specific schema and migration
notes; do not infer compatibility only from the binary version.

## Current preview limitations

The current release is a developer preview, not a production-readiness claim.
It provides at-least-once delivery, so subscribers must tolerate duplicate
notifications. Automatic subscription renewal, lease expiry, ownership
transfer and fencing, and complete failure qualification remain deferred.
macOS archives are not Apple-notarized and Windows archives are not
Authenticode-signed; portable checksum, Sigstore, SBOM, and provenance
verification is available on every platform.
