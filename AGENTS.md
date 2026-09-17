# WebSubHub repository guidance

## Product boundary

This repository implements WebSubHub. It composes the maintained
`github.com/ayeshLK/lib-websubhub` protocol framework and owns persistence,
delivery, clustering, security, operations, packaging, and release behavior.
Do not move those responsibilities into the library.

Maintain separate WebSub resource-topic and CloudEvents event-stream contracts.
WebSub conformance claims apply only to resource-topic behavior. Delivery is at
least once; never claim end-to-end exactly once.

## Architecture and invariants

- Keep MessageStore provider-neutral. Kafka implementations belong below
  `internal/persistence/messagestore/kafka`; StateStore must not import Kafka.
- Product state is event-sourced and versioned. Unknown persisted versions must
  fail closed and require an explicit offline migration; startup must not
  silently mutate schemas.
- The consolidator consumes durable state events and is the canonical snapshot
  source for management queries. Management handlers must not fall back to a
  hub-local projection when it is unavailable.
- Preserve exact message body bytes and complete topic media types. Topic
  content types are immutable contracts; publications must match them.
- Do not expose provider offsets, partitions, group IDs, handles, endpoint IDs,
  credentials, or other provider internals as product identities.

See @docs/architecture/decisions/README.md for the authoritative decisions and
product boundaries.

## Security and configuration

- Both hub listeners require an explicit `none` or `jwt` authentication mode;
  omission and invalid JWT configuration are startup errors, never a fallback
  to `none`. Disabled authentication uses actor ID `unauthenticated`.
- Callback verification, SSRF defenses, secret encryption, and provider
  security remain active regardless of API authentication mode.
- Kafka security supports TLS with server verification, optional client-cert
  mTLS, and SASL `plain`, `scram-sha-256`, or `scram-sha-512`; do not describe
  plaintext-only CI or Compose coverage as secured-broker qualification.
- Configuration is strict. Unknown TOML keys, unknown `WEBSUBHUB__...`
  overrides, invalid security settings, and incompatible process settings must
  fail startup.
- Never log payloads, authorization values, subscription secrets, callback
  capability queries, or provider credentials.

## Required workflow

Read relevant ADRs before changing behavior. Record a new ADR before
introducing a public contract, persisted schema, provider capability, security
boundary, or compatibility rule. Follow @CONTRIBUTING.md for issue, branch,
commit, and pull-request conventions.

Every Go source and test file must begin with the repository Apache-2.0 header.
Behavioral changes require deterministic tests; do not use timing sleeps.

Before handoff, run:

```sh
gofmt -w .
go vet ./...
go test -shuffle=on ./...
go test -race ./...
make license-check docs-check
git diff --check
```

Release versioning and tags are owned by the reviewed workflows and
`release.properties`. Never manually create, move, or delete release tags; read
@docs/releasing.md before release work.
