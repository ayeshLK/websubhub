# ADR 0021: Explicit API authentication modes

- Status: Accepted for v0.6.0
- Date: 2026-08-30
- Supersedes: The mandatory-JWT inbound endpoint decision in ADR 0016 and
  unconditional operations-scope requirement in ADR 0017

## Context

ADR 0016 required signed JWTs for all public protocol and protected operations
requests. That fail-closed profile is appropriate for production, but it makes
the downloadable developer preview depend on an external identity provider or
locally generated signing keys before an evaluator can register a topic,
publish content, create a subscription, or inspect the running hub.

Authentication is a deployment boundary owned by the WebSubHub product, not by
`lib-websubhub`. The protocol framework remains authentication-neutral. The
product must support a deliberate local-development mode without allowing a
missing or invalid production configuration to silently disable security.

## Decision

The public and operations HTTP listeners each require an explicit
authentication mode:

```toml
[server.auth]
mode = "none" # none or jwt

[operations.auth]
mode = "none" # none or jwt
```

An absent, empty, or unknown mode is a startup error. There is no implicit
runtime default and no fallback from `jwt` to `none`. The two listeners are
configured independently so deployments can keep operations protected or
network-isolated while selecting the appropriate public protocol policy.

In `jwt` mode, the listener uses the existing signed-JWT verifier and
operation-scope authorization. If either listener selects `jwt`, the complete
`security.jwt` issuer, audience, HTTPS JWKS, asymmetric algorithm, timing, and
token-size configuration is required and validated. Issuer or JWKS failure
continues to fail closed.

In `none` mode, the listener does not initialize or invoke JWT verification.
Its middleware passes requests through and authorization records the fixed
actor ID `unauthenticated`; it does not derive identity from request-controlled
values. All operations on that listener are accessible without a token.
Callback intent verification, callback SSRF policy, subscription-secret
encryption, payload bounds, and provider security remain enabled and
independent.

The configuration packaged in downloadable `websubhub` archives explicitly
sets both listeners to `none` to provide an immediately usable developer
experience. The production-oriented example explicitly selects `jwt` for both
listeners. Docker Compose acceptance continues to select `jwt` so the secured
path remains exercised end to end. Documentation and startup diagnostics must
warn that `none` is intended for local development or a separately protected
trusted boundary.

This is an intentional configuration compatibility break after the v0.5.0
developer preview. Existing configurations must add both mode declarations
before upgrading. The v0.5.1 release notes must call out that required change.

## Consequences

- A downloaded archive can be evaluated locally without generating JWTs.
- A configuration omission cannot silently expose either listener.
- Production deployments retain the existing issuer, signature, claim, and
  scope enforcement by explicitly selecting `jwt`.
- `none` authorizes the complete selected listener, so network exposure of
  that listener is an operator security decision and must be conspicuous.
- Adding another authentication mechanism requires extending the explicit
  mode contract and recording its trust and identity semantics.
