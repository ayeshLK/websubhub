# ADR 0012: Process-specific configuration roots

- Status: Accepted
- Date: 2026-08-26

## Context

The hub and consolidator share persistence concepts but have different runtime
responsibilities. A combined root accepts settings that one process cannot use,
makes secret distribution broader than necessary, and obscures which process
owns an internal TLS identity.

## Decision

WebSubHub has two canonical TOML configuration files and two typed root
structures:

- `websubhub.toml` configures the public hub process;
- `websubhub-consolidator.toml` configures the consolidator process.

The roots reuse shared typed definitions for MessageStore, Kafka, durations,
and certificate-file references. They do not reuse a permissive union root.
Each loader rejects keys belonging only to the other process.

Hub client authentication is nested below `consolidator.auth`. Consolidator
server authentication is nested below `server.auth`. Thus each process reads
only its own certificate and trust settings. Hierarchical environment overrides
remain relative to the selected root; for example,
`WEBSUBHUB__SERVER__ID` is valid for the hub and invalid for the consolidator.

## Consequences

- Deployments mount only the configuration and private keys needed by each
  process.
- Shared section changes are implemented once but validated in both roots.
- A setting moving between roots is a configuration compatibility change.
- ADR 0008 continues to define TOML and override syntax; its former combined
  root shape is replaced by this decision.
