# ADR 0008: TOML configuration and hierarchical environment overrides

- Status: Accepted; root shape amended by ADR 0012
- Date: 2026-08-26

## Decision

TOML is WebSubHub's canonical configuration-file format. Each binary consumes
a strict typed root, with shared typed sections reused between them as defined
by ADR 0012. Environment variables beginning with
`WEBSUBHUB__` override leaf values by mapping double-underscore-separated
segments to TOML paths; for example, `WEBSUBHUB__SERVER__ID` overrides
`server.id`.

Files and environment overrides are strict: unknown keys, unknown paths,
malformed values, incomplete security settings, and invalid combinations fail
startup. String overrides are literal; booleans, integers, durations, and arrays use
the destination field's canonical syntax. Effective configuration can be
rendered only through a redacted view; credentials and secret material are never
returned.

## Consequences

- Configuration compatibility is a product contract for each process-specific
  root.
- New fields require schema, validation, example, and redaction review.
- Environment names are case-insensitive after the fixed uppercase prefix, but
  documentation uses uppercase segments.
- There is no parallel YAML, JSON, or command-specific configuration model.
