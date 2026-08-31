# Release guide

WebSubHub releases `websubhub` and `websubhub-consolidator` independently under
one lockstep semantic version. This guide covers the `v0.5.x` developer-preview
contract accepted in
[ADR 0018](architecture/decisions/0018-release-distribution.md) and the automated
release authority accepted in
[ADR 0020](architecture/decisions/0020-automated-release-authority.md).

Maintainers do not invent or push release tags. `release.properties` declares
the reviewed development or release version. The `Prepare release` workflow
opens the stable-version pull request, and the protected `Release` workflow
verifies that merged version, creates the annotated tag, publishes the release,
and proposes the next patch snapshot.

## Published artifacts

A release contains 12 archives: separate component packages for `amd64` and
`arm64` on Linux, macOS, and Windows. Linux and macOS use `.tar.gz`; Windows
uses `.zip`. Every archive contains only one process:

```text
bin/<component>[.exe]
config/<component>.toml
README.md
LICENSE
NOTICE
```

The release also includes an SPDX JSON SBOM per archive, a SHA-256 checksum
manifest, a keyless Sigstore bundle for that manifest, and GitHub provenance
attestations.

The workflow publishes Linux `amd64`/`arm64` manifests to:

```text
docker.io/ayeshalmeida/websubhub:X.Y.Z
docker.io/ayeshalmeida/websubhub-consolidator:X.Y.Z
```

Preview releases do not move `latest`, major, or major/minor tags. Images run
as non-root, contain one component, and carry SBOM, provenance, and keyless
signatures.

## Docker Hub profile content

The version-controlled Docker Hub short descriptions and overview sources are:

- [`docs/dockerhub/websubhub.md`](dockerhub/websubhub.md);
- [`docs/dockerhub/websubhub-consolidator.md`](dockerhub/websubhub-consolidator.md).

The `Sync Docker Hub profiles` workflow publishes both profiles through the
protected `release` environment and its scoped `DOCKERHUB_TOKEN`. It runs after
a successful `Release` workflow and can also be dispatched manually from
`main` to publish reviewed corrections or recover a failed synchronization.
Profile synchronization does not publish, move, or delete image tags.

## One-time repository setup

Before the first release, a repository administrator must:

1. Confirm `ayeshalmeida/websubhub` and
   `ayeshalmeida/websubhub-consolidator` are public Docker Hub repositories.
2. Store a scoped Docker Hub access token with write access to both repositories
   as the `DOCKERHUB_TOKEN` secret in the GitHub `release` environment.
3. Protect the GitHub environment named `release`, require an appropriate
   reviewer, and restrict deployment to the `main` branch.
4. Enable **Allow GitHub Actions to create and approve pull requests** so the
   narrowly scoped release jobs can open preparation and next-development pull
   requests. The workflows never approve those pull requests.
5. Protect `v*` tags from manual deletion or updates while allowing the
   protected release workflow to create new tags.
6. Confirm GitHub OIDC is available for keyless signing and attestations.

The Docker Hub username is fixed in the workflow as `ayeshalmeida`; it is not
a configurable secret. Do not store passwords, private signing keys, or
long-lived GitHub tokens in the repository.

## Version lifecycle

The root `release.properties` file is the reviewed version declaration:

- `version=X.Y.Z-SNAPSHOT` means development toward `X.Y.Z`;
- the Prepare release workflow opens a pull request changing it to `X.Y.Z`;
- the Release workflow publishes exactly `vX.Y.Z`;
- successful publication opens a pull request for `X.Y.(Z+1)-SNAPSHOT`.

The first declaration is `0.5.0-SNAPSHOT`. Only strict stable `vX.Y.Z` tags
are comparison bases, and the declared candidate must be newer than the latest
one. To begin a minor or major line, change the snapshot through a normal pull
request before preparing the release. The parser, transitions, and invalid
cases run as part of `make check`.

## Local dry run

Install GoReleaser 2.18.x and Syft, then run:

```sh
make release-check
make release-snapshot
make container-check \
  VERSION=v0.5.0-dev COMMIT="$(git rev-parse HEAD)" \
  BUILD_DATE="$(git show -s --format=%cI HEAD)"
```

The snapshot builds and inspects all 12 archives but skips publication and
signing. The container check builds both production targets and executes their
`--version` command. Pull requests repeat both checks in CI.

## Publishing checklist

1. Review the GitHub product profile for the release. Confirm the repository
   description, topics, README positioning and current-version status, primary
   links, badges, and social preview accurately represent the shipped product.
   Review both Docker Hub short descriptions and overview sources for the same
   product version, supported tags and platforms, component responsibilities,
   configuration boundary, and security guidance.
   Revisit provider-specific wording such as "backed by Kafka" as the product
   gains additional supported profiles; do not let current implementation
   detail become a permanent product-identity constraint.
2. Confirm `main` contains the intended `X.Y.Z-SNAPSHOT`, CI passes, and
   [the preview disclosure](../.github/release-notes/developer-preview.md)
   accurately states limitations, at-least-once duplicate windows, and upgrade
   impact.
3. Run the local dry run with the pinned tool versions.
4. Open **Actions → Prepare release → Run workflow** and select `main`. Do not
   enter a version; the workflow reads `release.properties`.
5. Approve the generated pull request CI, review the one-line change from
   `X.Y.Z-SNAPSHOT` to `X.Y.Z`, and merge it after all checks pass.
6. Confirm both Docker Hub repositories are public, the scoped token works, the
   protected `release` environment allows `main`, and the `v*` tag rules are
   active.
7. Open **Actions → Release → Run workflow**, select `main`, and start it without
   entering a version.
8. Review the declared tag and exact source commit in the verification summary.
   Do not approve an unexpected candidate.
9. Approve the protected `release` environment after verification passes.
10. Confirm both image digests and all release assets, signatures, SBOMs, and
   attestations exist. The workflow publishes the GitHub draft only after both
   container publications succeed.
11. Confirm the automatically triggered `Sync Docker Hub profiles` workflow
    publishes both reviewed short descriptions and overviews. Dispatch it
    manually from `main` if synchronization needs to be retried.
12. Approve CI on the generated next-development pull request, verify its patch
    increment, and merge it.

Do not manually create, move, or delete a release tag. If failure occurs after
the tag is created but before any GitHub Release or image exists, fix the
workflow and dispatch it again. The tagged source remains the build input;
`main` may differ only in the documented release-automation and documentation
files. If the tag is not an ancestor, product code changed, or any release
record or other artifact exists, diagnose the partial state and use a reviewed
property change
plus the preparation workflow to consume the next unused version. If
publication succeeds but the next-development pull request fails, create that
same patch-snapshot change through a normal pull request.

## Consumer verification

Download an archive, the checksum file, and its `.sigstore.json` bundle. Check
the archive checksum first:

```sh
sha256sum --check websubhub_0.5.0_checksums.txt --ignore-missing
```

Verify the bundle against the protected workflow identity:

```sh
cosign verify-blob \
  --bundle websubhub_0.5.0_checksums.txt.sigstore.json \
  --certificate-identity \
    'https://github.com/ayeshLK/websubhub/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  websubhub_0.5.0_checksums.txt

gh attestation verify websubhub_0.5.0_linux_amd64.tar.gz \
  --repo ayeshLK/websubhub
```

Verify a container signature and provenance similarly:

```sh
cosign verify \
  --certificate-identity \
    'https://github.com/ayeshLK/websubhub/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  docker.io/ayeshalmeida/websubhub:0.5.0

gh attestation verify oci://docker.io/ayeshalmeida/websubhub:0.5.0 \
  --repo ayeshLK/websubhub
```

Repeat verification for the consolidator. Production deployments should pin
image digests, not tags. The certificate and attestation bind the exact source
revision even though the stable workflow identity names `main`.

The operator-facing [installation and deployment guide](installing.md) covers
archive selection, configuration placement, process startup order, and
container hardening. Keep it aligned whenever the release contract changes.

## Deferred native signing

Initial releases are not Apple Developer ID signed/notarized and do not carry
Windows Authenticode signatures. This is tracked by
[issue #7](https://github.com/ayeshLK/websubhub/issues/7). Checksums, Sigstore
signatures, SBOMs, and provenance apply to all six platforms meanwhile.
