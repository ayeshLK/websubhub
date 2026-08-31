# websubhub binary distribution

This archive contains the public protocol, state-projection, ingestion, and
delivery process for the Kafka BYOB developer preview.

## Contents

```text
bin/websubhub[.exe]
config/websubhub.toml
README.md
LICENSE
NOTICE
```

The included TOML file is a secure configuration template. Copy it to an
operator-owned location, replace every example endpoint and secret-file path,
and protect the resulting file. Credentials and private keys must be mounted
as files; do not add them to the archive directory.

Start Kafka and a same-version `websubhub-consolidator` before starting this
process:

```sh
./bin/websubhub --config /etc/websubhub/websubhub.toml
```

On Windows, use `bin\\websubhub.exe`. Run `bin/websubhub --version` to inspect
embedded version metadata.

The process writes newline-delimited JSON logs to standard error. Set
`logging.level` to `debug`, `info`, `warn`, or `error`; the default is `info`
and changes require a restart.

The hub and consolidator are independently deployable but released under one
lockstep product version. The hub-to-consolidator connection should use mTLS;
`none` is intended only for an isolated trusted network.

Verify this archive against the release checksum file before extraction, then
verify the checksum Sigstore bundle or GitHub provenance attestation as
described in the repository
[verification guide](https://github.com/ayeshLK/websubhub/blob/main/docs/releasing.md#consumer-verification).
The [installation guide](https://github.com/ayeshLK/websubhub/blob/main/docs/installing.md)
covers configuration placement, startup order, and container deployment.
The initial preview is not Apple-notarized or Authenticode-signed.
