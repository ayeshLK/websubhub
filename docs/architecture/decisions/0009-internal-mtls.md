# ADR 0009: Optional mTLS for the internal snapshot endpoint

- Status: Accepted for v0.5.0
- Date: 2026-08-26

## Decision

Hub-to-consolidator authentication has two explicit modes: `mtls` and `none`.
`mtls` is the recommended server-to-server mode. The consolidator requires its
server certificate and key plus a client CA; the hub requires a client
certificate and key plus the server CA. Hostname verification remains enabled,
with an optional configured server name for deployments whose endpoint host is
not present in the certificate.

`none` intentionally serves the internal endpoint without authentication for
trusted preview deployments. Omitting mTLS never causes a silent fallback:
partial certificate settings and mismatches between the selected mode and its
settings fail configuration validation.

## Consequences

- There is no preview bearer-token credential to distribute or log.
- Operators choosing `none` must keep the endpoint on a trusted network and
  accept that it is unauthenticated.
- Certificate files are loaded at process startup; live certificate rotation is
  deferred.
- Broader public API authentication and callback SSRF decisions remain open.
