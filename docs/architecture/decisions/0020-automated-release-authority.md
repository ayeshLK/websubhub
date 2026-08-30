# ADR 0020: Automated release authority

- Status: Accepted for v0.5.0
- Date: 2026-08-30

## Context

ADR 0018 makes a strict semantic-version tag the lockstep published version
authority for both product components. Manually choosing, creating, and pushing
that tag is an avoidable source of malformed versions, releases from the wrong
commit, and operator error. Inferring semantic intent from arbitrary commit or
pull-request titles would merely move that ambiguity into automation; the
repository does not yet require Conventional Commits.

A version declared in reviewed source gives maintainers and CI a deterministic
candidate before publication. The published tag must still agree with the
checked-out source, and publication must retain the protected approval,
immutable exact-version tags, draft-until-complete GitHub Release, portable
signing, and provenance controls accepted by ADR 0018.

## Decision

The repository root `release.properties` file declares the development or
release version. It contains one strict version property:

```properties
version=X.Y.Z-SNAPSHOT
```

`X.Y.Z-SNAPSHOT` denotes ongoing development toward `X.Y.Z`; `X.Y.Z` denotes a
prepared release candidate. Prerelease identifiers other than the literal
`-SNAPSHOT` suffix are rejected. The declared base version must be newer than
the highest existing stable `vX.Y.Z` tag.

A maintainer starts the `Prepare release` workflow from `main` without entering
a version. It reads the declared snapshot, validates the candidate, and opens a
pull request that removes `-SNAPSHOT`. The pull request therefore makes the
candidate explicit, reviewed, and subject to the normal CI and branch rules.
Changing the intended minor or major line happens beforehand through an
ordinary reviewed change, such as changing `0.5.1-SNAPSHOT` to
`0.6.0-SNAPSHOT`.

After the preparation pull request merges, a maintainer starts the `Release`
workflow from `main` without entering or creating a version. Before creating
the tag, the workflow requires the dispatched ref and current remote `main`
commit to match, requires `release.properties` to contain the corresponding
stable version, runs the full source checks, and runs the Kafka provider
integration suite. The protected `release` environment then requires approval.
Its publishing job creates and pushes an annotated tag for the verified commit
and uses that exact tag as the GoReleaser, binary, archive, image, signing, and
attestation version.

If an interrupted attempt created the tag but no GitHub Release, image, or
other published artifact, a later dispatch may resume that exact version. The
tagged commit must be an ancestor of current `main`, must declare the same
stable version, and remains the source that is tested and built. Changes after
the tag are limited to the release workflow, its version-validation scripts,
ADR 0020, and this release guide. Any product-code change, tag-target mismatch,
or existing GitHub Release rejects resumption. The workflow never moves or
recreates the tag. Any failure after a release record or another artifact
exists consumes the version.

After publication succeeds, the release workflow opens a second pull request
that increments only the patch component and restores `-SNAPSHOT`. For example,
publishing `0.5.0` proposes `0.5.1-SNAPSHOT`. A subsequent minor or major
development decision remains a normal reviewed property change.

Automation-created pull requests use the repository-scoped `GITHUB_TOKEN` with
explicit `contents: write` and `pull-requests: write` job permissions. The
repository must allow GitHub Actions to create pull requests. GitHub holds CI
runs created by this token for maintainer approval; the workflows do not use a
personal token, approve their own pull requests, or bypass branch protection.

The workflow uses a single non-cancelling release concurrency group. A failed
attempt never moves or recreates its tag. Apart from the narrow unpublished-tag
resume case, recovery requires a reviewed property change to the next unused
version. Tag rules must prevent manual deletion or updates while allowing the
protected release workflow to create new `v*` tags.

Release notes prepend a maintained developer-preview disclosure to GitHub's
generated change summary. The GitHub Release remains a draft until both images,
signatures, SBOMs, and provenance attestations succeed.

Because the workflow is dispatched from `main`, keyless signature workflow
identity is the release workflow at `refs/heads/main`; the certificate and
attestation still bind the exact source commit and generated subject digest.
Consumer verification must use that workflow identity rather than a tag-event
workflow identity.

## Consequences

- Maintainers approve the version through a source pull request and approve
  publication without typing or pushing a version tag.
- Checking out `vX.Y.Z` shows `version=X.Y.Z`, so source, artifacts, images, and
  the immutable published tag agree.
- Patch development advances automatically after a successful release; minor
  and major intent remains an explicit product decision.
- Two small generated pull requests surround a release, and their CI runs need
  maintainer approval when the repository token creates them.
- Release workflow changes require the same review and main-branch checks as
  product code.
- Keyless verification uses a stable workflow-on-main identity plus the exact
  certificate source revision, rather than a workflow identity containing the
  release tag.
