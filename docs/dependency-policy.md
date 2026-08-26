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
- Do not raise the Go 1.25.8 minimum solely because a newer local toolchain is
  available.
- Run vulnerability, secret, and license checks in CI. A suppression must be
  narrow, justified, reviewed, and time-bounded.

`lib-websubhub` is pinned to the released v0.6.0 version for the resource
protocol adapter. Upgrades require changelog review and protocol adapter tests;
the repository does not depend on an unpublished working tree.

## Reviewed direct dependencies

| Module | Pinned version | License | Purpose |
|---|---|---|---|
| `github.com/ayeshLK/lib-websubhub` | `v0.6.0` | Apache-2.0 | Bounded WebSub protocol parsing, verification, lifecycle callbacks, and one delivery attempt |
| `github.com/twmb/franz-go` | `v1.21.6` | BSD-3-Clause | Kafka producer, consumer, protocol, TLS/SASL integration |
| `github.com/twmb/franz-go/pkg/kadm` | `v1.18.0` | BSD-3-Clause | Kafka destination and consumer-group administration |

Kafka types remain inside the Kafka provider. The provider does not expose a
public plugin SDK or make Kafka identifiers part of product identity.
