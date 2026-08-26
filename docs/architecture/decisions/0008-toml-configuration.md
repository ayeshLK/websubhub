# ADR 0008: TOML configuration and hierarchical environment overrides

- Status: Accepted
- Date: 2026-08-26

## Decision

TOML is WebSubHub's canonical configuration-file format. Both binaries consume
one typed hierarchical Go schema. Environment variables beginning with
`WEBSUBHUB__` override leaf values by mapping double-underscore-separated
segments to TOML paths; for example, `WEBSUBHUB__SERVER__ID` overrides
`server.id`.

Files and environment overrides are strict: unknown keys, unknown paths,
malformed values, incomplete security settings, and invalid combinations fail
startup. String overrides are literal; booleans, integers, durations, and arrays use
the destination field's canonical syntax. Effective configuration can be rendered
only through a redacted view; credentials and secret material are never
returned.

## Consequences

- Configuration compatibility is a product contract even when an individual
  setting is used by only one binary.
- New fields require schema, validation, example, and redaction review.
- Environment names are case-insensitive after the fixed uppercase prefix, but
  documentation uses uppercase segments.
- There is no parallel YAML, JSON, or command-specific configuration model.
