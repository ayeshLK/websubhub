# ADR 0018: Lockstep component releases and distribution

- Status: Accepted for v0.5.0
- Date: 2026-08-28

## Context

The Kafka BYOB preview requires both `websubhub` and
`websubhub-consolidator`, but the processes have different responsibilities,
configuration roots, scaling models, and deployment lifecycles. A combined
archive or container would blur that boundary. Operators also need predictable
artifacts for Linux, macOS, and Windows without treating a source checkout as
a distribution.

The first releases do not have Apple Developer ID/notarization or Windows
Authenticode infrastructure. Portable integrity and provenance controls can
still protect every platform while native platform signing is added later.
The Docker Hub repository identity was previously an open implementation gate.

## Decision

A release tag versions both processes in lockstep. A `vX.Y.Z` source tag
produces version `vX.Y.Z` in both binaries and uses `X.Y.Z` in artifact and
container names. Components remain independently deployable and never share a
runtime archive or image.

Each release produces one archive for each component on all six initial
targets:

- Linux `amd64` and `arm64` as `.tar.gz`;
- macOS `amd64` and `arm64` as `.tar.gz`;
- Windows `amd64` and `arm64` as `.zip`.

Archive names use
`<component>_<version>_<os>_<arch>.<format>`. Each archive has a same-named
top-level directory containing:

```text
bin/<component>[.exe]
config/<component>.toml
README.md
LICENSE
NOTICE
```

The configuration is a secure template, not runnable credentials. A release
publishes SHA-256 checksums, an SPDX JSON SBOM for every archive, a keyless
Sigstore bundle for the checksum file, and GitHub build-provenance
attestations. GoReleaser performs reproducible cross-compilation with CGO
disabled, trimmed source paths, the tagged commit timestamp, and embedded
version, commit, and build date.

The two public Docker Hub repositories are fixed as:

- `ayeshalmeida/websubhub`;
- `ayeshalmeida/websubhub-consolidator`.

Each repository receives a Linux `amd64`/`arm64` OCI image containing only its
application binary and a minimal certificate-bearing, non-root runtime. The
preview publishes the exact `X.Y.Z` tag only. It does not publish mutable
`latest`, major, or major/minor tags. Images carry OCI source/version/revision
metadata, BuildKit SBOM and provenance attestations, GitHub provenance, and a
keyless Sigstore signature over the image digest.

Publication is tag-triggered, requires the tag to be strict semantic version
syntax and point to a commit contained in `main`, passes the complete source
and Kafka provider integration gates, and runs through the protected GitHub `release`
environment. The GitHub Release remains a draft until both Docker images,
signatures, and attestations succeed.

Apple Developer ID signing/notarization and Windows Authenticode are deferred
under [issue #7](https://github.com/ayeshLK/websubhub/issues/7). Their absence
must be stated in release documentation. Native installers and package-manager
repositories are outside the v0.5.0 contract.

## Consequences

- Operators can upgrade the two processes independently while selecting the
  same supported product version.
- A release contains 12 component archives rather than six combined archives;
  the additional build cost is small because Go cross-compilation is already
  required and both programs share the module cache.
- A compromised or partial Docker publication does not silently become the
  default because no mutable preview tag is moved.
- Docker Hub repositories must exist as public repositories and the release
  environment must hold a scoped access token before the first tag is pushed.
- Native operating-system trust prompts may remain until issue #7 is complete;
  users can instead verify checksums, Sigstore bundles, and provenance.
- A future change to version skew, artifact layout, platform scope, mutable
  tags, or signing policy requires a replacement ADR.
