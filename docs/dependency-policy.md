# Dependency policy

Dependencies are admitted deliberately because WebSubHub processes untrusted
HTTP, persists customer content and subscription secrets, and ships production
binaries.

- Prefer the Go standard library when it provides a clear, maintainable fit.
- Add a dependency only with a documented purpose, maintained upstream,
  compatible license, and review of its transitive graph and security history.
- Pin direct dependencies in `go.mod`; do not use branches or unreviewed local
  replacements in committed code.
- Allowed dependency licenses are Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC,
  and MIT unless an explicit legal review records another license.
- Keep provider SDKs behind internal provider packages.
- Do not raise the Go 1.25 minimum solely because a newer local toolchain is
  available.
- Run vulnerability, secret, and license checks in CI. A suppression must be
  narrow, justified, reviewed, and time-bounded.

`lib-websubhub` will be pinned to a released version when the protocol
composition slice starts; the repository does not depend on an unpublished
working tree.
