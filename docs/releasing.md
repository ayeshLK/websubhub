# Release guide

WebSubHub releases `websubhub` and `websubhub-consolidator` independently under
one lockstep semantic version. This guide covers the `v0.5.x` developer-preview
contract accepted in
[ADR 0018](architecture/decisions/0018-release-distribution.md).

Ordinary branch and pull-request workflows cannot publish. A strict `vX.Y.Z`
tag on a commit contained in `main` starts the protected release workflow.

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

1. Create `ayeshalmeida/websubhub` and
   `ayeshalmeida/websubhub-consolidator` as public Docker Hub repositories.
2. Create a scoped Docker Hub access token with write access to those
   repositories and store it as the GitHub Actions secret `DOCKERHUB_TOKEN`.
3. Create and protect the GitHub environment named `release`; require an
   appropriate reviewer and restrict deployment to release tags.
4. Protect `v*` tags from deletion or unauthorized updates.
5. Confirm GitHub OIDC is available for keyless signing and attestations.

The Docker Hub username is fixed in the workflow as `ayeshalmeida`; it is not
a configurable secret. Do not store passwords, private signing keys, or
long-lived GitHub tokens in the repository.

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

1. Confirm `main` CI passes, the worktree is clean, and release notes state
   preview limitations, at-least-once duplicate windows, and upgrade impact.
2. Run the local dry run with the pinned tool versions.
3. Confirm both Docker Hub repositories are public and the scoped token works.
4. Confirm the protected `release` environment is active.
5. Create and push an annotated, preferably signed, tag from the intended
   `main` commit:

   ```sh
   git tag -s v0.5.0 -m "WebSubHub v0.5.0"
   git push origin v0.5.0
   ```

6. Approve the protected deployment after verification passes.
7. Confirm both image digests and all release assets, signatures, SBOMs, and
   attestations exist. The workflow publishes the GitHub draft only after both
   container publications succeed.

Do not reuse a version tag after failure. Diagnose the draft and partial
registry state, fix the source, and release a new patch version. Never move an
already published tag.

## Consumer verification

Download an archive, the checksum file, and its `.sigstore.json` bundle. Check
the archive checksum first:

```sh
sha256sum --check websubhub_0.5.0_checksums.txt --ignore-missing
```

Verify the bundle against the exact workflow identity:

```sh
cosign verify-blob \
  --bundle websubhub_0.5.0_checksums.txt.sigstore.json \
  --certificate-identity \
    'https://github.com/ayeshLK/websubhub/.github/workflows/release.yml@refs/tags/v0.5.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  websubhub_0.5.0_checksums.txt

gh attestation verify websubhub_0.5.0_linux_amd64.tar.gz \
  --repo ayeshLK/websubhub
```

Verify a container signature and provenance similarly:

```sh
cosign verify \
  --certificate-identity \
    'https://github.com/ayeshLK/websubhub/.github/workflows/release.yml@refs/tags/v0.5.0' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  docker.io/ayeshalmeida/websubhub:0.5.0

gh attestation verify oci://docker.io/ayeshalmeida/websubhub:0.5.0 \
  --repo ayeshLK/websubhub
```

Repeat verification for the consolidator. Production deployments should pin
image digests, not tags.

## Deferred native signing

Initial releases are not Apple Developer ID signed/notarized and do not carry
Windows Authenticode signatures. This is tracked by
[issue #7](https://github.com/ayeshLK/websubhub/issues/7). Checksums, Sigstore
signatures, SBOMs, and provenance apply to all six platforms meanwhile.
