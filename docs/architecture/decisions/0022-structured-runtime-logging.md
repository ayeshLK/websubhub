# ADR 0022: Structured runtime logging

- Status: Accepted for the next development version after v0.6.1
- Date: 2026-08-31

## Context

The hub and consolidator need machine-readable operational records with an
operator-selectable verbosity. Ad hoc text diagnostics are difficult to query,
while unconstrained structured fields can disclose protocol payloads, callback
capabilities, authorization material, or provider details and can create
unbounded-cardinality telemetry.

Logging configuration is a shared product concern, but runtime records remain
component-specific. Readiness continues to be defined by the health endpoints;
a startup log must not claim listener readiness before listeners are serving.

## Decision

Both process-specific roots contain the same explicit setting:

```toml
[logging]
level = "info" # debug, info, warn, or error
```

The default is `info`. Values are exact and lower case; an absent value receives
the default, while an empty or unknown value fails configuration validation.
`WEBSUBHUB__LOGGING__LEVEL` overrides the TOML leaf under ADR 0008. The setting
is read only at startup and changing it requires a process restart.

Runtime records are newline-delimited JSON written to standard error. The
destination and format are fixed for this preview. `--version`, flag usage, and
flag parser errors remain human-readable command output rather than runtime
telemetry.

Each runtime logger carries a fixed `component`. Records use `time`, `level`,
and `msg`, plus a reviewed allowlist of bounded, low-cardinality attributes.
Unknown attributes are dropped. Runtime and configuration failures expose a
controlled `error_class`, not arbitrary parser or provider error text. Log records must never
contain payloads, authorization values, subscription secrets, callback URLs or
capability queries, provider credentials, response bodies, or Kafka offsets,
partitions, and group IDs as product identities. URL-valued topic identifiers
are not part of the generic logging allowlist.

Startup records distinguish `runtime_initialized` from readiness. Projection
or state-service catch-up may be recorded after its startup barrier completes,
but health endpoints remain authoritative for listener and dependency
readiness. Clean shutdown and terminal runtime failure are recorded once by the
process command boundary.

HTTP access, protocol-operation, delivery-attempt, and provider instrumentation
will be added separately using this schema. Hot-path success records should be
`debug`; warnings and errors are reserved for actionable degradation or
failure.

## Consequences

- Operators can select `debug`, `info`, `warn`, or `error` consistently for
  either component and through the existing environment override mechanism.
- Container runtimes can collect JSON records directly from standard error.
- New fields require an explicit safety and cardinality review before joining
  the allowlist.
- Dynamic level changes, alternate formats, files, syslog, and remote exporters
  are not supported by this preview contract.
