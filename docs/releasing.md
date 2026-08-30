# Release guide

WebSubHub releases `websubhub` and `websubhub-consolidator` independently under
one lockstep semantic version. This guide covers the `v0.5.x` developer-preview
contract accepted in
[ADR 0018](architecture/decisions/0018-release-distribution.md) and the automated
release authority accepted in
[ADR 0020](architecture/decisions/0020-automated-release-authority.md).

Maintainers do not invent or push release tags. The protected `Release` GitHub
Actions workflow calculates the version, verifies the current `main` commit,
creates the annotated tag, and publishes the release.

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

## One-time repository setup

Before the first release, a repository administrator must:

1. Confirm `ayeshalmeida/websubhub` and
   `ayeshalmeida/websubhub-consolidator` are public Docker Hub repositories.
2. Store a scoped Docker Hub access token with write access to both repositories
   as the `DOCKERHUB_TOKEN` secret in the GitHub `release` environment.
3. Protect the GitHub environment named `release`, require an appropriate
   reviewer, and restrict deployment to the `main` branch.
4. Protect `v*` tags from manual deletion or updates while allowing the
   protected release workflow to create new tags.
5. Confirm GitHub OIDC is available for keyless signing and attestations.

The Docker Hub username is fixed in the workflow as `ayeshalmeida`; it is not
a configurable secret. Do not store passwords, private signing keys, or
long-lived GitHub tokens in the repository.

## Version calculation

The workflow accepts semantic intent rather than a version string:

- with no existing stable release tag, every choice produces the fixed first
  version `v0.5.0`;
- `patch` increments `vX.Y.Z` to `vX.Y.(Z+1)`;
- `minor` increments `vX.Y.Z` to `vX.(Y+1).0`;
- `major` increments `vX.Y.Z` to `v(X+1).0.0`.

Only strict stable `vX.Y.Z` tags are version bases. The calculator and its
edge cases run as part of `make check`.

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

1. Confirm `main` CI passes, the worktree is clean, and
   [the preview disclosure](../.github/release-notes/developer-preview.md)
   accurately states limitations, at-least-once duplicate windows, and upgrade
   impact.
2. Run the local dry run with the pinned tool versions.
3. Confirm both Docker Hub repositories are public and the scoped token works.
4. Confirm the protected `release` environment allows only `main`, and the
   `v*` tag rules are active.
5. Open **Actions → Release → Run workflow**, select `main`, choose the intended
   `patch`, `minor`, or `major` semantic change, and start the workflow.
6. Review the calculated tag and exact source commit in the verification job
   summary. Do not approve an unexpected candidate.
7. Approve the protected `release` environment after verification passes.
8. Confirm both image digests and all release assets, signatures, SBOMs, and
   attestations exist. The workflow publishes the GitHub draft only after both
   container publications succeed.

Do not manually create a release tag. Do not reuse or move a version tag after
failure. Diagnose the draft and partial registry state, fix the source, merge
the correction, and dispatch a new release using `patch` so a new version is
consumed.

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
