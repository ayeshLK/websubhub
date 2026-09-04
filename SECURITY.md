# Security policy

## Supported versions

WebSubHub v0.7.0 is a developer preview, not a supported production release.
Security fixes are made on the main development branch and will be published in
subsequent preview releases.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for the repository. If
that channel is unavailable, contact the repository owner privately. Do not
open a public issue with exploit details, credentials, callback URLs, payloads,
or subscription secrets.

Include the affected revision, impact, reproduction steps, and any suggested
mitigation. You should receive an acknowledgement within seven days; a
severity-based remediation and disclosure timeline will be agreed after
triage.

## Current status

The repository has published the `v0.7.0` Kafka BYOB developer preview. It
provides explicit unauthenticated or JWT listener modes, JWT authentication and
scope authorization for secured deployments, callback SSRF defenses, encrypted
persisted subscription secrets, durable state and content handling,
at-least-once delivery, and operational inspection. It has not completed
failure qualification or production-hardening gates and must not be treated as
a supported production deployment.
