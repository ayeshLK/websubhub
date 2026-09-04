# Contributing to WebSubHub

WebSubHub is in active pre-release development. Open an issue before proposing
a public HTTP contract, persisted schema, provider abstraction, or security
boundary change.

## Local validation

Use Go 1.26.8 or newer, then run:

```sh
make check
```

The command checks formatting, generated drift, vet findings, unit tests, race
behavior, dependency licenses, and local Markdown links. CI additionally runs
vulnerability and secret scanning.

Every behavioral change must include deterministic tests. Avoid timing sleeps;
use contexts, channels, barriers, and bounded deadlines. Preserve exact HTTP
methods, status codes, headers, content types, and payload bytes in wire-level
tests.

## Scope and compatibility

- Keep WebSub resource topics and CloudEvents event streams as separate
  contracts.
- Keep product persistence and delivery behavior out of `lib-websubhub`.
- Keep provider identifiers out of product identities and public APIs.
- Treat delivery as at least once and document duplicate windows.
- Require an ADR and migration/recovery fixtures before changing persisted
  schemas.
- Do not claim complete WebSub product conformance from library conformance
  alone.

See the [architecture decision index](docs/architecture/decisions/README.md)
before changing product boundaries.
