# ADR 0020: Automated release authority

- Status: Accepted for v0.5.0
- Date: 2026-08-30

## Context

ADR 0018 makes a strict semantic-version tag the lockstep version authority for
both product components. Manually choosing, creating, and pushing that tag is
an avoidable source of malformed versions, releases from the wrong commit, and
operator error. Inferring semantic intent from arbitrary commit or pull-request
titles would merely move that ambiguity into automation; the repository does
not yet require Conventional Commits.

Publication must retain the protected approval, immutable exact-version tags,
draft-until-complete GitHub Release, portable signing, and provenance controls
accepted by ADR 0018.

## Decision

Releases start only through the `Release` GitHub Actions workflow dispatched
from `main`. The maintainer chooses semantic intent (`patch`, `minor`, or
`major`) rather than entering a version or creating a tag.

The workflow calculates the candidate from the highest strict stable `vX.Y.Z`
tag. When no release tag exists, the candidate is fixed as `v0.5.0` regardless
of the selected bump. Otherwise patch increments `Z`, minor increments `Y` and
resets `Z`, and major increments `X` and resets both lower components.
Prerelease tags are not release bases in this initial automation contract.

Before creating the candidate tag, the workflow requires the dispatched ref and current
remote `main` commit to match, rejects an existing candidate tag, runs the full
source checks, and runs the Kafka provider integration suite. The protected
`release` environment then requires approval. Its publishing job creates and
pushes an annotated tag for the verified commit and uses that exact tag as the
GoReleaser, binary, archive, image, signing, and attestation version.

The workflow uses a single non-cancelling release concurrency group. A failed
attempt never moves or reuses its tag; the next dispatch calculates from that
tag and therefore consumes a new semantic version. Tag rules must prevent
manual deletion or updates while allowing the protected release workflow to
create new `v*` tags.

Release notes prepend a maintained developer-preview disclosure to GitHub's
generated change summary. The GitHub Release remains a draft until both images,
signatures, SBOMs, and provenance attestations succeed.

Because the workflow is dispatched from `main`, keyless signature workflow
identity is the release workflow at `refs/heads/main`; the certificate and
attestation still bind the exact source commit and generated subject digest.
Consumer verification must use that workflow identity rather than a tag-event
workflow identity.

## Consequences

- Maintainers approve semantic intent and publication without typing or pushing
  a version tag.
- The first automated run is deterministically `v0.5.0`; later runs cannot
  collide with or move an existing stable tag.
- Choosing the semantic bump remains an explicit product decision until the
  repository adopts and enforces a machine-readable change convention.
- Release workflow changes require the same review and main-branch checks as
  product code.
- Keyless verification uses a stable workflow-on-main identity plus the exact
  certificate source revision, rather than a workflow identity containing the
  release tag.
