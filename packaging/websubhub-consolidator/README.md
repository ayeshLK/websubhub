# websubhub-consolidator binary distribution

This archive contains the deterministic state reduction and canonical snapshot
process for the Kafka BYOB developer preview.

## Contents

```text
bin/websubhub-consolidator[.exe]
config/websubhub-consolidator.toml
README.md
LICENSE
NOTICE
```

The included TOML file is a secure configuration template. Copy it to an
operator-owned location, replace every example endpoint and secret-file path,
and protect the resulting file. Credentials and private keys must be mounted
as files; do not add them to the archive directory.

Start Kafka before starting this process:

```sh
./bin/websubhub-consolidator \
  --config /etc/websubhub/websubhub-consolidator.toml
```

On Windows, use `bin\\websubhub-consolidator.exe`. Run the binary with
`--version` to inspect embedded version metadata.

The consolidator and hub are independently deployable but released under one
lockstep product version. Use mTLS for hub clients unless the internal listener
is isolated on a trusted network.

Verify this archive against the release checksum file before extraction, then
verify the checksum Sigstore bundle or GitHub provenance attestation as
described in the repository
[release guide](https://github.com/ayeshLK/websubhub/blob/main/docs/releasing.md).
The initial preview is not Apple-notarized or Authenticode-signed.
