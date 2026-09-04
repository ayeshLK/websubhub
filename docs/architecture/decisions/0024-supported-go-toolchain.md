# ADR 0024: Supported Go toolchain floor

- Status: Accepted for v0.7.0
- Date: 2026-09-03
- Supersedes: The Go 1.25 minimum in ADR 0007

## Context

The preview originally used Go 1.25.8 as its minimum and container build
toolchain. Go 1.27 has since been released, ending support for the Go 1.25
release line. A vulnerability scan of the exact Go 1.25.8 toolchain identified
reachable standard-library vulnerabilities in HTTP, TLS, URL parsing, and
certificate validation paths used by WebSubHub.

Go 1.26.8 is a supported patch release containing the relevant security fixes.
It is also the last Go release line that runs on macOS 12, so selecting it
preserves a broader host compatibility boundary than raising the minimum
directly to Go 1.27.

## Decision

WebSubHub requires Go 1.26.8 or newer. The production container build uses the
official `golang:1.26.8-alpine3.23` image pinned by its multi-platform digest.
Release archive builds and security checks use exactly Go 1.26.8.

Continuous integration tests the exact minimum and the latest patch of the
newest supported Go release line. This keeps the minimum reproducible while
detecting forward-compatibility regressions. The release target matrix remains
Linux, macOS, and Windows on `amd64` and `arm64`; this decision does not change
the artifact layout, persisted schemas, network contracts, or product APIs.

A later security patch within the Go 1.26 release line may replace 1.26.8
without another ADR. Raising the minimum to a later Go release line, or making
a change that narrows the supported operating-system targets, requires a new
or replacement compatibility decision.

## Consequences

- Builds made with the previously accepted Go 1.25 toolchain are no longer
  supported.
- Contributors and release automation must use Go 1.26.8 or newer.
- Production images and newly published archives no longer contain the
  standard-library vulnerabilities reported against Go 1.25.8.
- Go 1.27 remains covered in CI without imposing its macOS 13 toolchain floor
  on contributors or release automation.
